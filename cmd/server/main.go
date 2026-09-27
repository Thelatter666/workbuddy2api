// main.go workbuddy2api 入口：加载配置、构建 pool、起调度器与 HTTP 服务。
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"workbuddy2api/internal/auth"
	"workbuddy2api/internal/metrics"
	"workbuddy2api/internal/notify"
	"workbuddy2api/internal/pool"
	"workbuddy2api/internal/redisstore"
	"workbuddy2api/internal/scheduler"
	"workbuddy2api/internal/server"
	"workbuddy2api/internal/session"
	"workbuddy2api/internal/upstream"
)

func main() {
	cfgPath := flag.String("config", "config.json", "path to config json")
	flag.Parse()

	cfg, err := Load(*cfgPath)
	if err != nil {
		// 配置文件不存在时给一次机会用纯默认 + env
		if os.IsNotExist(err) {
			log.Printf("config %s not found, using defaults+env", *cfgPath)
			cfg, err = Load("")
		}
		if err != nil {
			log.Fatalf("load config: %v", err)
		}
	}

	auths, err := auth.LoadDir(cfg.AuthDir)
	if err != nil {
		log.Fatalf("load auths: %v", err)
	}
	log.Printf("loaded %d account(s) from %s", len(auths), cfg.AuthDir)

	// global realm 路由开关（config global.enabled，缺省 true）：注入 auth 包全局闸。
	// Realm()/IsGlobal() 先过此闸——显式 false 时恒 cn（逃生门：纯 CN 锁定的第一道闸）。
	auth.SetGlobalEnabled(cfg.Global.Enabled)

	// redisstore：未配置/连接失败 → Noop（纯内存模式，一切功能照常）。
	store := redisstore.New(cfg.Upstash.URL, cfg.Upstash.Token)

	p := pool.New(cfg.StateFile)
	defer p.Flush() // 进程退出前强制落盘（后台 flush 每 5s 一次，退出时补一次）
	p.SetStore(store)
	p.RestoreFromSnapshot() // 择新恢复：Redis 快照比本地新才采用，否则本地优先
	p.SyncToDir(auths)      // 与 auths 目录对齐：新账号加入、已删除文件账号剔除（状态保留）

	// 熔断器 + 在途上限 + 三因子加权调优（从 config 注入，非正值回退默认）。
	p.SetBreaker(cfg.Pool.BreakerThreshold, cfg.BreakerCooldownDur, cfg.BreakerCooldownMaxD)
	p.SetMaxInFlight(cfg.Pool.MaxInFlight)
	p.SetSoftRateMax(cfg.SoftRateMaxDur) // 软冷却指数退避封顶（soft_rate_max，默认 2h）
	p.SetWeights(cfg.Pool.IdleWeightPerHour, cfg.Pool.IdleWeightMax)

	// 会话粘性路由（可配关闭）。
	var sessRouter *session.Router
	redisMode := "noop"
	if _, ok := store.(redisstore.Noop); !ok {
		redisMode = "upstash"
	}
	if cfg.SessionSticky.Enabled {
		sessRouter = session.New(session.Config{
			TTL:        cfg.SessionTTL,
			GCInterval: cfg.SessionGCInterval,
			Store:      store,
			Available:  p.AvailableUIDs,
			// 按模型的可用性口径：绑定号在当前模型被 6004 限额时重分配，
			// 而不是被钉在这个号上反复失败。
			// realm 感知闭包：带前缀模型名按 realm 过滤可用账号（跨 realm 不泄漏，
			// 见 wiring.go）；裸名走 cn（现状零回归）。
			AvailableForModel: realmAwareAvailableForModel(p),
		})
		sessRouter.LoadFromStore() // 启动时从 Redis 恢复粘性（读操作仅此处）
		sessRouter.StartGC()
		defer sessRouter.StopGC()
	}
	sessCount := func() int {
		if sessRouter != nil {
			return sessRouter.Count()
		}
		return 0
	}

	up := upstream.New()
	// 短 RPC 总时长上限（refresh/checkin/balance/FetchModels），语义不变。
	up.HTTP.Timeout = time.Duration(cfg.Upstream.TimeoutSeconds) * time.Second
	// 聊天 SSE 首字节前（响应头）上限：cfg 已 normalize（缺省回落 timeout_seconds）。
	up.HeaderTimeout = time.Duration(cfg.Upstream.HeaderTimeoutSeconds) * time.Second
	if tr, ok := up.ChatHTTP.Transport.(*http.Transport); ok {
		tr.ResponseHeaderTimeout = up.HeaderTimeout
	}
	// 聊天 SSE 流中空闲上限（S3 空闲监控读取）。
	up.IdleTimeout = time.Duration(cfg.Upstream.IdleTimeoutSeconds) * time.Second
	up.SanitizeFingerprints = cfg.Features.SanitizeBlacklistFingerprints
	// 出站 UA（A 段）：非空才做显式覆盖，空 = 默认 WorkBuddy 三段式
	// `WorkBuddy/<client_version> WorkBuddy/<client_version> CLI/<cli_version>`。
	up.UserAgent = cfg.Upstream.UserAgent
	// 版本段（upstream.client_version / cli_version）：空 = 各走内置默认。
	up.ClientVersion = cfg.Upstream.ClientVersion
	up.CliVersion = cfg.Upstream.CliVersion
	// 设备风控头（X-Device-Token）全局兜底 + 文件读取路径；空 = 不注入。
	up.DeviceToken = cfg.Upstream.DeviceToken
	up.DeviceTokenFile = cfg.Upstream.DeviceTokenFile
	// 用量归属头（X-Product/X-IDE-*）+ 客户端 IP 透传开关（见 ChatHeaders / handler）。
	up.ClientName = cfg.Upstream.ClientName
	up.PassthroughIP = cfg.Upstream.PassthroughIP
	// global realm 双域路由（config global 段）：base 空回落内置默认 https://www.workbuddy.ai；
	// GlobalEnabled 与 auth 包开关一致（双保险第二道闸在 upstream.globalOn）。
	up.ChatBaseGlobal = cfg.Global.ChatBase
	up.BillingBaseGlobal = cfg.Global.BillingBase
	up.GlobalEnabled = cfg.Global.Enabled

	// 异常推送出口：面板是拉取式的（只有人打开浏览器才有意义），而「余额低 / 签到失败 /
	// 池子停摆」恰恰是人没在看的时候发生的。enabled 与 token 缺一即不构造 → nil，
	// 调度器侧零开销、不发任何请求。
	var notifier *notify.Notifier
	switch {
	case !cfg.Notify.Enabled:
		log.Printf("异常推送已禁用（notify.enabled=false）")
	case cfg.Notify.Token == "":
		log.Printf("异常推送未启用：notify.enabled=true 但 notify.token 为空（可用 WB2A_PUSHPLUS_TOKEN 注入）")
	default:
		notifier = notify.New(notify.Config{
			Token:    cfg.Notify.Token,
			Cooldown: time.Duration(cfg.Notify.CooldownHours) * time.Hour,
		})
		log.Printf("异常推送已启用：签到失败 / 余额低（单号<%d 或全池<%d）/ 池子停摆 → 微信（同一告警 %dh 内不重推）",
			cfg.Notify.BalanceLowPerAccount, cfg.Notify.BalanceLowTotal, cfg.Notify.CooldownHours)
	}

	sch := scheduler.New(scheduler.Config{
		Pool:                 p,
		Upstream:             up,
		Notify:               notifier,
		BalanceLowPerAccount: cfg.Notify.BalanceLowPerAccount,
		BalanceLowTotal:      cfg.Notify.BalanceLowTotal,
		CheckinHours:         cfg.Schedule.CheckinHours,
		TravelHours:          cfg.Schedule.TravelHours,
		ActivityHours:        cfg.Schedule.ActivityHours,
		KeepaliveHours:       cfg.Schedule.KeepaliveHours,
		SchoolHours:          cfg.Schedule.SchoolHours,
		CatHours:             cfg.Schedule.CatHours,
		ActivityReportCount:  cfg.Schedule.ActivityReportCount,
		// 余额刷新：分钟 → Duration（0 = 关闭）。面板「剩余积分」与 picker 的 credits
		// 权重都读池子这份值，而它原本只在签到时刷新，最长滞后 12 小时。
		CreditRefreshInterval: time.Duration(cfg.Schedule.CreditRefreshMinutes) * time.Minute,
		CheckinDisabled:       !cfg.Schedule.CheckinEnabled,
		TravelDisabled:        !cfg.Schedule.TravelEnabled,
		ActivityDisabled:      !cfg.Schedule.ActivityEnabled,
		KeepaliveDisabled:     !cfg.Schedule.KeepaliveEnabled,
		SchoolDisabled:        !cfg.Schedule.SchoolEnabled,
		CatDisabled:           !cfg.Schedule.CatEnabled,
	})
	switch {
	case !cfg.Schedule.CheckinEnabled:
		log.Printf("签到已禁用（schedule.checkin_enabled=false）")
	default:
		log.Printf("签到已启用：%v 点（签到 + 余额查询解冻）", cfg.Schedule.CheckinHours)
	}
	switch {
	case !cfg.Schedule.TravelEnabled:
		log.Printf("猫猫旅行已禁用（schedule.travel_enabled=false）")
	default:
		log.Printf("猫猫旅行已启用：%v 点（独立排程：领养 / 派出 / 领奖）", cfg.Schedule.TravelHours)
	}
	switch {
	case !cfg.Schedule.ActivityEnabled:
		log.Printf("活跃上报已禁用（schedule.activity_enabled=false）")
	default:
		log.Printf("活跃上报已启用：%v 点（每号 %d 条，点亮连登 + 补满领猫对话门槛）", cfg.Schedule.ActivityHours, cfg.Schedule.ActivityReportCount)
	}
	if !cfg.Schedule.KeepaliveEnabled {
		log.Printf("token 保活已禁用（schedule.keepalive_enabled=false）")
	} else {
		log.Printf("token 保活已启用：%v 点", cfg.Schedule.KeepaliveHours)
	}
	if !cfg.Schedule.SchoolEnabled {
		log.Printf("开学季任务已禁用（schedule.school_enabled=false）")
	} else {
		log.Printf("开学季任务已启用：%v 点（school_open_day_2026.py ALL --run --yes）", cfg.Schedule.SchoolHours)
	}
	if !cfg.Schedule.CatEnabled {
		log.Printf("夜猫子任务已禁用（schedule.cat_enabled=false）")
	} else {
		log.Printf("夜猫子任务已启用：%v 点（task_runner.py ALL --yes --only black_cat）", cfg.Schedule.CatHours)
	}
	if cfg.Schedule.CreditRefreshMinutes <= 0 {
		log.Printf("余额定期刷新已禁用（schedule.credit_refresh_minutes=0）：面板「剩余积分」只在签到刷新时更新")
	} else {
		log.Printf("余额定期刷新已启用：每 %d 分钟（面板「剩余积分」与选号 credits 权重据此更新）", cfg.Schedule.CreditRefreshMinutes)
	}

	// 统计采集器：内存滚动窗口（小时 7 天 / 日 30 天）+ 跨年长期日台账，
	// 分别按 metrics_file / ledger_file 落盘回放，使面板的今日与长期用量、积分
	// 消耗跨重启连续（进程累计仍重启清零）。
	// 面板关闭时不构造（handler 侧 Metrics=nil，零开销）。
	var collector *metrics.Collector
	if cfg.Dashboard.Enabled {
		collector = metrics.NewWithStateLedger(cfg.Dashboard.MetricsFile, cfg.Dashboard.LedgerFile)
		defer collector.Flush() // 进程退出前强制落盘（后台 flush 每 5s 一次，退出时补一次）
		// 余额来源：面板要「积分余额趋势」，只能靠定期快照——上游余额只在签到/查询时
		// 刷新，而逐请求只拿得到消耗量（usage.credit）。采集器按日留存收盘值。
		collector.SetBalanceSource(func() map[string]int64 {
			list := p.List()
			out := make(map[string]int64, len(list))
			for _, st := range list {
				out[st.UID] = st.Credits
			}
			return out
		})
	}

	h := server.NewHandler(server.Config{
		Pool:         p,
		Upstream:     up,
		APIKey:       cfg.APIKey,
		Session:      sessRouter,
		StickyCount:  sessCount,
		RedisMode:    redisMode,
		SoftCooldown: cfg.SoftRateDur,
		PromptMode:   cfg.Prompt.Mode,
		PromptText:   cfg.PromptText,
		MaxBodyBytes: int64(cfg.Server.MaxBodyMB) << 20, // MB → 字节
		// global realm 开关（handler 侧第三道闸：modelList 据此决定是否列 global 名单）。
		GlobalEnabled:           cfg.Global.Enabled,
		Metrics:                 collector,
		DashboardEnabled:        cfg.Dashboard.Enabled,
		DashboardRefreshSeconds: cfg.Dashboard.RefreshSeconds,
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go sch.Run(ctx)

	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           h,
		ReadHeaderTimeout: 30 * time.Second,
	}
	go func() {
		<-ctx.Done()
		p.Flush() // 信号触发：先落盘再做优雅停机
		if collector != nil {
			collector.Flush() // 统计历史同样先落盘（否则本次进程数据白记）
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	if cfg.Global.Enabled {
		log.Printf("global realm 已启用（chat_base=%q billing_base=%q，空=默认 workbuddy.ai）",
			cfg.Global.ChatBase, cfg.Global.BillingBase)
	} else {
		log.Printf("global realm 已禁用（config global.enabled=false，纯 CN）")
	}
	log.Printf("workbuddy2api listening on %s (api_key=%v)", cfg.Listen, cfg.APIKey != "")
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("http: %v", err)
	}
	log.Printf("bye")
}
