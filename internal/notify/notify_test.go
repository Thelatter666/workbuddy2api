package notify

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// capture 假 pushplus：记录收到的请求体与 Content-Type，按 code 返回业务码。
type capture struct {
	calls atomic.Int32
	last  atomic.Value // map[string]string
	ctype atomic.Value // string
	code  int
}

func (c *capture) server() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.calls.Add(1)
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		c.last.Store(body)
		c.ctype.Store(r.Header.Get("Content-Type"))
		code := c.code
		if code == 0 {
			code = 200
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"code": code, "msg": "发送成功"})
	}))
}

func (c *capture) body() map[string]string {
	v, _ := c.last.Load().(map[string]string)
	return v
}

// TestDisabledWithoutToken 未配 token = 零成本空操作：一个请求都不许发。
// 「没配 token」是静默降级，不该报错也不该产生流量。
func TestDisabledWithoutToken(t *testing.T) {
	c := &capture{}
	srv := c.server()
	defer srv.Close()

	n := New(Config{Token: "", Endpoint: srv.URL})
	if n.Enabled() {
		t.Error("空 token 时 Enabled() 应为 false")
	}
	n.Send("k", "t", "c")

	if got := c.calls.Load(); got != 0 {
		t.Errorf("upstream calls=%d want 0（未配 token）", got)
	}
}

// TestSendPostsHTMLPayload 正常推送：payload 带 token/title/content/template=html。
func TestSendPostsHTMLPayload(t *testing.T) {
	c := &capture{}
	srv := c.server()
	defer srv.Close()

	n := New(Config{Token: "tok", Endpoint: srv.URL})
	n.Send("k1", "✅ 测试标题", "第一行<br><b>加粗</b>")

	if got := c.calls.Load(); got != 1 {
		t.Fatalf("calls=%d want 1", got)
	}
	if ct, _ := c.ctype.Load().(string); ct != "application/json" {
		t.Errorf("Content-Type=%q want application/json", ct)
	}
	b := c.body()
	for k, want := range map[string]string{
		"token": "tok", "title": "✅ 测试标题", "content": "第一行<br><b>加粗</b>", "template": "html",
	} {
		if b[k] != want {
			t.Errorf("payload[%s]=%q want %q", k, b[k], want)
		}
	}
}

// TestSendCooldownSuppressesRepeatSameKey 冷却期内同 key 只发一次，换 key 照发。
// 这是「余额低」这类持续状态不变成轰炸的关键；冷却过后要能重推（提醒还没解决）。
func TestSendCooldownSuppressesRepeatSameKey(t *testing.T) {
	c := &capture{}
	srv := c.server()
	defer srv.Close()

	n := New(Config{Token: "tok", Endpoint: srv.URL, Cooldown: time.Hour})
	n.Send("balance_low:u1", "t", "c")
	n.Send("balance_low:u1", "t", "c") // 冷却内 → 静默
	if got := c.calls.Load(); got != 1 {
		t.Fatalf("同 key 重复推送 calls=%d want 1", got)
	}

	n.Send("balance_low:u2", "t", "c") // 不同 key 不受影响
	if got := c.calls.Load(); got != 2 {
		t.Fatalf("换 key 后 calls=%d want 2", got)
	}

	// 模拟冷却已过：把上次时刻往前拨，同 key 应重推。
	n.mu.Lock()
	n.last["balance_low:u1"] = time.Now().Add(-2 * time.Hour)
	n.mu.Unlock()
	n.Send("balance_low:u1", "t", "c")
	if got := c.calls.Load(); got != 3 {
		t.Errorf("冷却过后 calls=%d want 3（应重推一次）", got)
	}
}

// TestSendFailureConsumesCooldown 发送失败不得 panic/返回错误，且同样消耗冷却窗口：
// 否则 pushplus 不可用时会退化成每一轮调度都重试一次。
func TestSendFailureConsumesCooldown(t *testing.T) {
	c := &capture{code: 500} // 业务码失败（HTTP 仍是 200）
	srv := c.server()
	defer srv.Close()

	n := New(Config{Token: "tok", Endpoint: srv.URL, Cooldown: time.Hour})
	n.Send("k", "t", "c")
	n.Send("k", "t", "c")

	if got := c.calls.Load(); got != 1 {
		t.Errorf("失败后同 key 重试 calls=%d want 1（冷却应照常消耗）", got)
	}
}

// TestSendHTTPErrorNotFatal 传输层失败（连不上）：Send 不 panic、不阻塞调用方。
func TestSendHTTPErrorNotFatal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close() // 立即关掉 → 连接被拒

	n := New(Config{Token: "tok", Endpoint: url, Timeout: time.Second})
	n.Send("k", "t", "c") // 只记日志，不 panic
}
