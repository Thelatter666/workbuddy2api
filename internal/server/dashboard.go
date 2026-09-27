// dashboard.go 统计面板：/dashboard 返回内嵌单文件前端，/api/dashboard 返回
// 池快照 + 请求统计（metrics 采集器）的聚合 JSON。
package server

import (
	_ "embed"
	"net/http"
	"strconv"

	"workbuddy2api/internal/metrics"
)

// dashboardHTML 内嵌单文件面板（离线可用：无任何外部请求，图表为手写 SVG）。
//
//go:embed dashboard.html
var dashboardHTML []byte

// dashboardPage 返回面板外壳（公开、不含数据；数据由 /api/dashboard 按需鉴权拉取）。
func (h *Handler) dashboardPage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	// 面板是无状态外壳且可能随版本更新：禁止缓存，避免升级后浏览器仍跑旧 JS。
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(dashboardHTML)
}

// dashboardAPI 聚合面板数据：池实时快照（与 /status 同源同字段）+ 请求统计。
// 实时 credit 仍以 `./credit` 上游查询为准，此处不打上游（避免每次刷新 N×200ms 阻塞）。
//
// 查询参数 days=N 控制长期序列（long_term）的窗口长度，默认 90 天、上限见
// metrics.LedgerDays（3 年）。热数据（小时环/日环）始终全量返回，不受该参数影响。
func (h *Handler) dashboardAPI(w http.ResponseWriter, r *http.Request) {
	total, healthy, cooling, disabled, inFlightFull := h.cfg.Pool.CountsDetailed()
	sticky := 0
	if h.cfg.StickyCount != nil {
		sticky = h.cfg.StickyCount()
	}
	redisMode := h.cfg.RedisMode
	if redisMode == "" {
		redisMode = "noop"
	}
	days := dashboardDaysParam(r)
	var m any
	persistent, ledgerPersistent := false, false
	if h.cfg.Metrics != nil {
		m = h.cfg.Metrics.SnapshotDays(days)
		persistent = h.cfg.Metrics.Persistent()
		ledgerPersistent = h.cfg.Metrics.LedgerPersistent()
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"service": ServiceName,
		"status": map[string]any{
			"accounts":       h.cfg.Pool.List(),
			"total":          total,
			"healthy":        healthy,
			"cooling":        cooling,
			"disabled":       disabled,
			"in_flight_full": inFlightFull,
			"realm_totals": map[string]map[string]int{
				"cn":     countsMapFrom(h.cfg.Pool.CountsDetailedForRealm("cn")),
				"global": countsMapFrom(h.cfg.Pool.CountsDetailedForRealm("global")),
			},
			"sticky_sessions": sticky,
			"redis_mode":      redisMode,
		},
		"metrics": m,
		// 落盘状态供页脚如实说明「重启是否清零」——开着落盘却说清零就是撒谎。
		"metrics_persistent": persistent,
		"ledger_persistent":  ledgerPersistent,
		"days":               days,
		"days_max":           metrics.LedgerDays,
		"refresh_seconds":    h.cfg.DashboardRefreshSeconds,
		"auth_required":      h.cfg.APIKey != "",
	})
}

// dashboardDaysParam 解析长期序列窗口（天）。非法/缺省一律回落 defaultDays；
// 上限钳到 metrics.LedgerDays——请求方不能靠参数把响应体撑到任意大。
func dashboardDaysParam(r *http.Request) int {
	const defaultDays = 90
	n := defaultDays
	if v := r.URL.Query().Get("days"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			n = parsed
		}
	}
	if n < 1 {
		n = defaultDays
	}
	if n > metrics.LedgerDays {
		n = metrics.LedgerDays
	}
	return n
}
