// dashboard_test.go 统计面板路由测试：外壳 embed、API 鉴权、JSON 结构、开关关闭 404。
package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"workbuddy2api/internal/auth"
	"workbuddy2api/internal/metrics"
	"workbuddy2api/internal/upstream"
)

func testAuth(uid string) *auth.Auth {
	// ExpiresAt 取远期：token 视为有效，不触发 refresh（refresh 需要 refreshToken，
	// 缺失时 pick 会判 no_healthy_account）。对齐 handler_test.go 的既有约定。
	return &auth.Auth{UID: uid, AccessToken: "tok-" + uid, ExpiresAt: 9999999999, Nickname: "n-" + uid}
}

// TestDashboardPageServesEmbeddedHTML 断言 /dashboard 返回内嵌 HTML（公开，无需鉴权）。
func TestDashboardPageServesEmbeddedHTML(t *testing.T) {
	p := testPoolWith(testAuth("u1"))
	h := NewHandler(Config{Pool: p, Upstream: upstream.New(), DashboardEnabled: true, Metrics: metrics.New()})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/dashboard", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Errorf("Content-Type=%q, want text/html", ct)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "WorkBuddy2API 用量看板") {
		head := body
		if len(head) > 120 {
			head = head[:120]
		}
		t.Errorf("外壳未包含标题: %q", head)
	}
	// embed 必须真的加载进来（防 //go:embed 路径写错导致空文件）。
	if len(dashboardHTML) == 0 {
		t.Fatal("dashboardHTML embed 为空")
	}
	// 离线约定：不得引用任何外部资源。
	if strings.Contains(body, "http://") || strings.Contains(body, "https://") || strings.Contains(body, "cdn.") {
		t.Error("面板引用了外部资源，违反离线约定")
	}
}

// TestDashboardDisabledReturns404 断言未开启时路由不存在（Config 零值零回归）。
func TestDashboardDisabledReturns404(t *testing.T) {
	p := testPoolWith(testAuth("u1"))
	h := NewHandler(Config{Pool: p, Upstream: upstream.New()}) // 未设 DashboardEnabled

	for _, path := range []string{"/dashboard", "/api/dashboard"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
		if rec.Code != http.StatusNotFound {
			t.Errorf("GET %s status=%d, want 404", path, rec.Code)
		}
	}
}

// TestDashboardAPIRequiresAuthWhenKeySet 断言数据端点与 /status 同鉴权口径。
func TestDashboardAPIRequiresAuthWhenKeySet(t *testing.T) {
	p := testPoolWith(testAuth("u1"))
	h := NewHandler(Config{Pool: p, Upstream: upstream.New(), APIKey: "secret",
		DashboardEnabled: true, Metrics: metrics.New()})

	// 无 key → 401
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/api/dashboard", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("无 key status=%d, want 401", rec.Code)
	}

	// 正确 key → 200
	rec = httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/dashboard", nil)
	req.Header.Set("Authorization", "Bearer secret")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("带 key status=%d, want 200 (body=%s)", rec.Code, rec.Body)
	}
}

// TestDashboardAPIShapeAndMetrics 断言聚合 JSON 结构：status 快照 + metrics + 配置透出。
func TestDashboardAPIShapeAndMetrics(t *testing.T) {
	p := testPoolWith(testAuth("u1"), testAuth("u2"))
	col := metrics.New()
	// 注入一条样本，确认 API 透出的是采集器真实数据。
	col.Record(metrics.Sample{
		Start: time.Now(), Model: "glm-5.2", Mode: "stream", UID: "u1",
		Status: 200, Tokens: 100, PromptTokens: 50, Credit: 1.25, HasCredit: true,
		TTFB: 200 * time.Millisecond,
	})

	h := NewHandler(Config{Pool: p, Upstream: upstream.New(),
		DashboardEnabled: true, DashboardRefreshSeconds: 15, Metrics: col})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/api/dashboard", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200", rec.Code)
	}
	var body struct {
		Service        string `json:"service"`
		RefreshSeconds int    `json:"refresh_seconds"`
		AuthRequired   bool   `json:"auth_required"`
		Status         struct {
			Accounts []map[string]any `json:"accounts"`
			Total    int              `json:"total"`
			Healthy  int              `json:"healthy"`
		} `json:"status"`
		Metrics struct {
			Total struct {
				Requests int64   `json:"requests"`
				OK       int64   `json:"ok"`
				Tokens   int64   `json:"tokens"`
				Credit   float64 `json:"credit"`
			} `json:"total"`
			Hours []map[string]any `json:"hours"`
			Days  []map[string]any `json:"days"`
		} `json:"metrics"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("JSON 解析失败: %v\n%s", err, rec.Body.String())
	}
	if body.Service != ServiceName {
		t.Errorf("service=%q, want %q", body.Service, ServiceName)
	}
	if body.RefreshSeconds != 15 {
		t.Errorf("refresh_seconds=%d, want 15", body.RefreshSeconds)
	}
	if body.AuthRequired {
		t.Error("auth_required=true，但未配置 APIKey")
	}
	if len(body.Status.Accounts) != 2 || body.Status.Total != 2 {
		t.Errorf("accounts=%d total=%d, want 2/2", len(body.Status.Accounts), body.Status.Total)
	}
	if body.Metrics.Total.Requests != 1 || body.Metrics.Total.Tokens != 150 || body.Metrics.Total.Credit != 1.25 {
		t.Errorf("metrics.total=%+v, want req=1 tok=150 credit=1.25", body.Metrics.Total)
	}
	if len(body.Metrics.Hours) != metrics.HourSlots || len(body.Metrics.Days) != metrics.DaySlots {
		t.Errorf("metrics hours=%d days=%d, want %d/%d",
			len(body.Metrics.Hours), len(body.Metrics.Days), metrics.HourSlots, metrics.DaySlots)
	}
}

// TestDashboardAPIRefreshDefault 断言 refresh_seconds 未配置时回落 10。
func TestDashboardAPIRefreshDefault(t *testing.T) {
	p := testPoolWith(testAuth("u1"))
	h := NewHandler(Config{Pool: p, Upstream: upstream.New(), DashboardEnabled: true, Metrics: metrics.New()})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/api/dashboard", nil))
	var body struct {
		RefreshSeconds int `json:"refresh_seconds"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("JSON 解析失败: %v", err)
	}
	if body.RefreshSeconds != 10 {
		t.Errorf("refresh_seconds=%d, want 10（兜底）", body.RefreshSeconds)
	}
}

// TestDashboardAPIMetricsPersistentFlag 断言落盘状态如实透出：页脚据此措辞，
// 开着落盘却报 false 会让面板谎称「重启即清零」。
func TestDashboardAPIMetricsPersistentFlag(t *testing.T) {
	p := testPoolWith(testAuth("u1"))

	// 纯内存采集器（New）→ false
	h := NewHandler(Config{Pool: p, Upstream: upstream.New(), DashboardEnabled: true, Metrics: metrics.New()})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/api/dashboard", nil))
	var body struct {
		MetricsPersistent bool `json:"metrics_persistent"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("JSON 解析失败: %v", err)
	}
	if body.MetricsPersistent {
		t.Error("纯内存采集器 metrics_persistent 应为 false")
	}

	// 带落盘路径的采集器 → true
	fp := filepath.Join(t.TempDir(), "metrics.json")
	h2 := NewHandler(Config{Pool: p, Upstream: upstream.New(), DashboardEnabled: true,
		Metrics: metrics.NewWithState(fp)})
	rec2 := httptest.NewRecorder()
	h2.ServeHTTP(rec2, httptest.NewRequest("GET", "/api/dashboard", nil))
	if err := json.Unmarshal(rec2.Body.Bytes(), &body); err != nil {
		t.Fatalf("JSON 解析失败: %v", err)
	}
	if !body.MetricsPersistent {
		t.Error("带落盘路径的采集器 metrics_persistent 应为 true")
	}
}

// TestChatRequestFeedsMetrics 端到端：一次成功 chat 请求必须落入采集器。
// 这是埋点 defer 的回归保护——若 Record 未接上，面板永远空白。
func TestChatRequestFeedsMetrics(t *testing.T) {
	auths := []*auth.Auth{testAuth("u1")}
	p := testPoolWith(auths...)
	col := metrics.New()
	up := newFakeUpstream(t, func(string) (int, string, bool) { return 200, sseOK, true })
	h := NewHandler(Config{Pool: p, Upstream: up, Metrics: col})

	req := httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"glm-5.2","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("chat status=%d, want 200 (body=%s)", rec.Code, rec.Body)
	}

	s := col.Snapshot()
	if s.Total.Requests != 1 {
		t.Fatalf("采集 requests=%d, want 1", s.Total.Requests)
	}
	if s.Total.OK != 1 {
		t.Errorf("采集 ok=%d, want 1", s.Total.OK)
	}
	if s.Total.Tokens != 2 { // sseOK: prompt=1 + completion=1
		t.Errorf("采集 tokens=%d, want 2", s.Total.Tokens)
	}
	if s.ByUID["u1"].Requests != 1 || s.ByModel["glm-5.2"].Requests != 1 {
		t.Errorf("维度未记录: uid=%+v model=%+v", s.ByUID, s.ByModel)
	}
}

// TestDashboardAPIDaysWindow 断言长期序列窗口：默认 90 天、按参数裁剪、超界钳到台账上限，
// 且窗口长度与平行数组长度严格一致（前端直接按数组下标取数，错位就是画错图）。
func TestDashboardAPIDaysWindow(t *testing.T) {
	p := testPoolWith(testAuth("u1"))
	col := metrics.New()
	h := NewHandler(Config{Pool: p, Upstream: upstream.New(), DashboardEnabled: true, Metrics: col})

	parse := func(t *testing.T, url string) (days int, daysMax int, dates, credit, reqs int) {
		t.Helper()
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", url, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status=%d, want 200", url, rec.Code)
		}
		var body struct {
			Days    int `json:"days"`
			DaysMax int `json:"days_max"`
			Metrics struct {
				LongTerm struct {
					Dates    []string  `json:"dates"`
					Credit   []float64 `json:"credit"`
					Requests []int64   `json:"requests"`
				} `json:"long_term"`
			} `json:"metrics"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("%s JSON 解析失败: %v", url, err)
		}
		lt := body.Metrics.LongTerm
		return body.Days, body.DaysMax, len(lt.Dates), len(lt.Credit), len(lt.Requests)
	}

	if days, max, dates, credit, reqs := parse(t, "/api/dashboard"); days != 90 || max != metrics.LedgerDays ||
		dates != 90 || credit != 90 || reqs != 90 {
		t.Errorf("默认窗口 days=%d max=%d dates=%d credit=%d reqs=%d，期望 90/%d 且长度一致",
			days, max, dates, credit, reqs, metrics.LedgerDays)
	}
	if days, _, dates, _, _ := parse(t, "/api/dashboard?days=7"); days != 7 || dates != 7 {
		t.Errorf("days=7 时 days=%d dates=%d，期望都是 7", days, dates)
	}
	// 超上限必须钳住，不能由请求方决定响应体大小。
	if days, _, dates, _, _ := parse(t, "/api/dashboard?days=99999"); days != metrics.LedgerDays || dates != metrics.LedgerDays {
		t.Errorf("days=99999 时 days=%d dates=%d，期望钳到 %d", days, dates, metrics.LedgerDays)
	}
	// 非法值回落默认，而不是报错或返回空窗口。
	if days, _, dates, _, _ := parse(t, "/api/dashboard?days=abc"); days != 90 || dates != 90 {
		t.Errorf("days=abc 时 days=%d dates=%d，期望回落 90", days, dates)
	}
	if days, _, dates, _, _ := parse(t, "/api/dashboard?days=0"); days != 90 || dates != 90 {
		t.Errorf("days=0 时 days=%d dates=%d，期望回落 90", days, dates)
	}
}

// TestDashboardAPILedgerPersistentFlag 断言长期台账落盘状态如实透出（页脚据此措辞）。
func TestDashboardAPILedgerPersistentFlag(t *testing.T) {
	dir := t.TempDir()
	p := testPoolWith(testAuth("u1"))
	col := metrics.NewWithStateLedger(filepath.Join(dir, "metrics.json"), filepath.Join(dir, "ledger.json"))
	h := NewHandler(Config{Pool: p, Upstream: upstream.New(), DashboardEnabled: true, Metrics: col})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/api/dashboard", nil))
	var body struct {
		LedgerPersistent bool `json:"ledger_persistent"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("JSON 解析失败: %v", err)
	}
	if !body.LedgerPersistent {
		t.Error("带台账路径的采集器 ledger_persistent 应为 true")
	}
}
