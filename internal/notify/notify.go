// Package notify 异常事件推送：把「需要人知道」的事情推到微信（Pushplus）。
//
// 为什么需要它：面板是拉取式的，只有在有人打开浏览器的那一刻才有意义。而余额耗尽、
// 签到失败、池子停摆这三类事情的本质恰恰是「人没在看的时候发生了」——2026-09-27
// 的实际教训是：21:00 签到因 DNS 全批失败（当晚回补没了）、次日 08:00 起三小时烧掉
// 9400 credit 后流量归零，全程没有任何东西通知任何人，要靠人事后去翻日志才发现。
//
// 设计取向（对齐本仓库 stdlib 优先）：
//   - 单文件、零外部依赖，只用 net/http + sync；
//   - 同一个 key 在冷却期内只推一次：余额低是**持续状态**，而检查是每 10 分钟一轮，
//     不去重就会变成 144 条/天的轰炸；冷却过后仍然异常则重推一次——「还没解决」
//     本身也是信息。key 的粒度由调用方决定（如签到失败按自然日做 key = 每天至多一条）；
//   - 发送失败只记日志、绝不返回错误给调用方：推送是旁路，不能拖慢调度或请求。
package notify

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// DefaultEndpoint Pushplus 推送接口（测试用 Config.Endpoint 注入假服务）。
const DefaultEndpoint = "https://www.pushplus.plus/send"

// defaultTimeout 单次推送的超时：pushplus 抖动不该挂住调用方（调度器主循环）。
const defaultTimeout = 10 * time.Second

// Config 通知器配置。
type Config struct {
	// Token Pushplus token；空 = 未配置（Notifier 退化为空操作）。
	Token string
	// Endpoint 覆盖推送接口地址（仅测试用；空则用 DefaultEndpoint）。
	Endpoint string
	// Cooldown 同一 key 的重推冷却；<=0 回落 DefaultCooldown。
	Cooldown time.Duration
	// Timeout 单次推送超时；<=0 回落 defaultTimeout。
	Timeout time.Duration
}

// DefaultCooldown 默认重推冷却（6 小时）。
//
// 取值理由：最坏情况（余额低 + 池子停摆同时存在）下每天至多各 4 条，是「会被看、
// 不会烦」的量级；而异常本身是持续数小时到数天的状态，6 小时后重推一次足够提醒
// 「还没好」，不会因为冷却太长而彻底漏掉。
const DefaultCooldown = 6 * time.Hour

// Notifier 推送器。零值不可用，一律经 New 构造。
type Notifier struct {
	token    string
	endpoint string
	cooldown time.Duration
	http     *http.Client

	mu   sync.Mutex
	last map[string]time.Time // key → 上次**尝试**推送的时刻
}

// New 构造推送器。token 为空时返回一个空操作实例（Enabled() == false），
// 让调用方不必到处判空——「没配 token」应当是零成本的静默，而不是错误。
func New(cfg Config) *Notifier {
	ep := cfg.Endpoint
	if ep == "" {
		ep = DefaultEndpoint
	}
	cd := cfg.Cooldown
	if cd <= 0 {
		cd = DefaultCooldown
	}
	to := cfg.Timeout
	if to <= 0 {
		to = defaultTimeout
	}
	return &Notifier{
		token:    cfg.Token,
		endpoint: ep,
		cooldown: cd,
		http:     &http.Client{Timeout: to},
		last:     map[string]time.Time{},
	}
}

// Enabled 是否已配置可用的 token。
func (n *Notifier) Enabled() bool { return n != nil && n.token != "" }

// Send 推送一条消息；同一 key 在冷却期内静默跳过。
//
// key 决定去重粒度，由调用方按事件语义选择：
//   - 持续状态（余额低、池子停摆）→ 固定 key，冷却期后自动重推；
//   - 偶发事件（签到失败）→ 带自然日的 key，每天至多一条。
//
// title/content 走 Pushplus 的 html 模板，content 里可用 <br>/<b>。
func (n *Notifier) Send(key, title, content string) {
	if !n.Enabled() {
		return
	}
	n.mu.Lock()
	if at, ok := n.last[key]; ok && time.Since(at) < n.cooldown {
		n.mu.Unlock()
		return
	}
	// 记录在**尝试**之前：冷却的契约是「同一 key 不在窗口内重复发」，而不是
	// 「成功才计次」。pushplus 短暂不可用时按 cooldown 重试；被吞掉的那条会由
	// 持续状态在下一个冷却窗口重新触发（余额低/池子停摆都不会自己消失），
	// 而如果改成「失败不计次」，链路不通时就会退化成每轮刷屏。
	n.last[key] = time.Now()
	n.mu.Unlock()

	if err := n.push(title, content); err != nil {
		log.Printf("WARN: [notify] push failed key=%s: %v", key, err)
		return
	}
	log.Printf("[notify] pushed key=%s title=%s", key, title)
}

// push 真正发一次 HTTP 请求。仅由 Send 调用（冷却已在外层判定）。
func (n *Notifier) push(title, content string) error {
	body, err := json.Marshal(map[string]string{
		"token":    n.token,
		"title":    title,
		"content":  content,
		"template": "html",
	})
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	resp, err := n.http.Post(n.endpoint, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("post: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("http %d", resp.StatusCode)
	}
	// Pushplus 走 200 + 业务码：HTTP 通了但 code != 200 仍是失败（如 token 失效），
	// 只看状态码会把「token 配错了」误报成发送成功。
	var r struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	if r.Code != 200 {
		return fmt.Errorf("pushplus code=%d msg=%s", r.Code, r.Msg)
	}
	return nil
}
