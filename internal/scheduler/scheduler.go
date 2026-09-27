// Package scheduler 定时任务：签到 / 活跃上报 / 猫猫旅行 / token keepalive / 开学季 / 夜猫子
// 六类整点排程，外加一条间隔驱动的「余额定期刷新」。
// 签到成功后重新查余额，余额 > 0 的冷却账号自动解冻（解冻只归签到，余额刷新不动冷却）。
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"workbuddy2api/internal/auth"
	"workbuddy2api/internal/logfmt"
	"workbuddy2api/internal/notify"
	"workbuddy2api/internal/pool"
	"workbuddy2api/internal/upstream"
)

// Config 调度器依赖。
//
// 任务开关用「禁用」命名而非「启用」：零值 Config 即六类任务都启用（hours 回落默认），
// 与引入开关前的行为逐字一致（老调用方/老测试无需改动）。
type Config struct {
	Pool           *pool.Pool
	Upstream       *upstream.Client
	CheckinHours   []int // 默认 [9, 21]
	TravelHours    []int // 默认 [9,21]：一趟派出 + 一趟领奖闭环
	ActivityHours  []int // 默认 [10]
	KeepaliveHours []int // 默认 [22]
	SchoolHours    []int // 默认 [12]：开学季任务（迁移自系统 crontab）
	CatHours       []int // 默认 [1]：夜猫子任务（迁移自系统 crontab）
	// ActivityReportCount 每号每次活跃上报的条数：领猫前置需 5 次对话，
	// 默认 5 条同一 conversationId 内多轮上报把 chat_5 刷满；0/缺省=1 兼容旧行为。
	ActivityReportCount int

	// CreditRefreshInterval 余额刷新间隔；0 = 关闭该刷新（恢复旧行为：只在签到时刷余额）。
	//
	// 池子的 credits 唯一写入点是签到（每天 2 次），请求消耗从不递减，故它是一份
	// 「上次签到时的读数」：实测最长滞后 12 小时；2026-09-27 实测滞后 5 小时，
	// 面板显示 10933 而上游真实 5792（高估 89%，其中 Elysia 号 910 vs 224）。
	// 面板「剩余积分」与 picker 的 credits 权重都读这份值，所以拿它做独立间隔排程。
	// 用间隔而非整点小时（对比 CheckinHours 等）：滞后量级要压到分钟级才有意义，
	// 整点排程做不到；也因此它不进 nextWake，由 Run 里独立的 ticker 驱动——
	// 六类整点任务全禁用时，余额刷新仍应照常运行。
	//
	// 余额类告警（余额低 / 池子停摆）挂在同一轮 tick 上：它们要的正是这份新鲜读数，
	// 且同样需要分钟级检查频率。故本项为 0 时，余额类告警也一并关闭。
	CreditRefreshInterval time.Duration

	// Notify 异常推送出口；nil = 不推送（零开销）。只推三类需要人介入的事：
	// 签到失败、余额低、池子无法服务。
	Notify *notify.Notifier
	// BalanceLowPerAccount / BalanceLowTotal 余额告警阈值（0 = 不检查该项）。
	BalanceLowPerAccount int64
	BalanceLowTotal      int64

	// CheckinDisabled 显式关闭签到排程（对应 config 的 schedule.checkin_enabled=false）。
	// 禁用后不再有任何签到时点。旅行不再搭签到便车（已剥离为独立排程）。
	CheckinDisabled bool
	// TravelDisabled 显式关闭猫猫旅行排程（schedule.travel_enabled=false）。
	TravelDisabled bool
	// ActivityDisabled 显式关闭活跃上报排程（schedule.activity_enabled=false）。
	ActivityDisabled bool
	// KeepaliveDisabled 显式关闭 token 保活排程（schedule.keepalive_enabled=false）。
	KeepaliveDisabled bool
	// SchoolDisabled 显式关闭开学季任务排程（schedule.school_enabled=false）。
	SchoolDisabled bool
	// CatDisabled 显式关闭夜猫子任务排程（schedule.cat_enabled=false）。
	CatDisabled bool
}

// Scheduler 调度器。
type Scheduler struct {
	cfg Config

	// mu/adoptTried 领养当日失败记录：uid → 自然日（CST）。门槛未达的账号当日不再重试，
	// 避免同日多趟对上游重试轰炸；进程重启即清零（无需持久化）。
	mu         sync.Mutex
	adoptTried map[string]string

	// checkinMu 串行化签到：定时入口与手动触发互斥，避免同一时刻重复打上游签到接口。
	checkinMu sync.Mutex
}

// New 构建。
func New(cfg Config) *Scheduler {
	if len(cfg.CheckinHours) == 0 {
		cfg.CheckinHours = []int{9, 21}
	}
	if len(cfg.TravelHours) == 0 {
		cfg.TravelHours = []int{9, 21}
	}
	if len(cfg.ActivityHours) == 0 {
		cfg.ActivityHours = []int{10}
	}
	if len(cfg.KeepaliveHours) == 0 {
		cfg.KeepaliveHours = []int{22}
	}
	if len(cfg.SchoolHours) == 0 {
		cfg.SchoolHours = []int{12}
	}
	if len(cfg.CatHours) == 0 {
		cfg.CatHours = []int{1}
	}
	// 0/缺省 = 1 条（兼容旧行为：每号每天 1 条上报点亮连登）。
	if cfg.ActivityReportCount <= 0 {
		cfg.ActivityReportCount = 1
	}
	return &Scheduler{cfg: cfg, adoptTried: make(map[string]string)}
}

// checkinRefreshSkew 签到前判定"token 是否临近过期"的时间窗口（10 分钟）。
// 长时间停机/容器长期停跑后 access token 往往已过期，不先刷新则签到必然 401 白跑。
const checkinRefreshSkew = 10 * time.Minute

// creditRefreshAccountGap 余额刷新时账号之间的间隔：与 cmd/credit 的限速一致，
// 避免同一实例连续打上游触发风控（刷新间隔本身是分钟级，这点延迟无所谓）。
const creditRefreshAccountGap = 200 * time.Millisecond

// CheckinStatus 单账号签到结果状态。
type CheckinStatus string

const (
	CheckinOK      CheckinStatus = "ok"      // 签到成功
	CheckinAlready CheckinStatus = "already" // 上游判定今天已签到（幂等重复，视为正常）
	CheckinFail    CheckinStatus = "fail"    // 刷新 token / 签到 / 余额查询失败
	CheckinSkipped CheckinStatus = "skipped" // 禁用账号或无有效凭证，未参与
)

// CheckinOutcome 单账号签到结果（供手动签到回执与日志汇总）。
type CheckinOutcome struct {
	UID      string         `json:"uid"`
	Nickname string         `json:"nickname,omitempty"`
	Status   CheckinStatus  `json:"status"`
	Credits  *int64         `json:"credits,omitempty"` // 签到后余额（余额查询成功才有值）
	Detail   string         `json:"detail,omitempty"`  // 失败/跳过原因（"已签到"不填）
}

// ErrBusy 已有一次签到正在执行（手动入口与定时撞车）。
var ErrBusy = errors.New("checkin already running")

// nextFire 返回 now 之后最近的一个整点触发时间；hours 为本地小时（0-23）。
func nextFire(now time.Time, hours []int) time.Time {
	var earliest time.Time
	for _, h := range hours {
		t := time.Date(now.Year(), now.Month(), now.Day(), h, 0, 0, 0, now.Location())
		if !t.After(now) {
			t = t.Add(24 * time.Hour)
		}
		if earliest.IsZero() || t.Before(earliest) {
			earliest = t
		}
	}
	return earliest
}

// taskKind 调度任务类型。
type taskKind int

const (
	taskCheckin taskKind = iota
	taskTravel
	taskActivity
	taskKeepalive
	taskSchool
	taskCat
)

// nextWake 返回 now 之后最近的唤醒时刻，以及该时刻需要执行的全部任务。
// 多类任务若配到同一小时（如签到与旅行都含 9），该时刻多类任务需一并执行。
// 已显式禁用的任务不进候选（nextFire 对其零值返回零时间，nextWake 再跳过零时点）。
func (s *Scheduler) nextWake(now time.Time) (time.Time, []taskKind) {
	type slot struct {
		at   time.Time
		kind taskKind
	}
	var slots []slot
	if !s.cfg.CheckinDisabled {
		slots = append(slots, slot{nextFire(now, s.cfg.CheckinHours), taskCheckin})
	}
	if !s.cfg.TravelDisabled {
		slots = append(slots, slot{nextFire(now, s.cfg.TravelHours), taskTravel})
	}
	if !s.cfg.ActivityDisabled {
		slots = append(slots, slot{nextFire(now, s.cfg.ActivityHours), taskActivity})
	}
	if !s.cfg.KeepaliveDisabled {
		slots = append(slots, slot{nextFire(now, s.cfg.KeepaliveHours), taskKeepalive})
	}
	if !s.cfg.SchoolDisabled {
		slots = append(slots, slot{nextFire(now, s.cfg.SchoolHours), taskSchool})
	}
	if !s.cfg.CatDisabled {
		slots = append(slots, slot{nextFire(now, s.cfg.CatHours), taskCat})
	}
	var earliest time.Time
	for _, sl := range slots {
		if sl.at.IsZero() {
			continue
		}
		if earliest.IsZero() || sl.at.Before(earliest) {
			earliest = sl.at
		}
	}
	if earliest.IsZero() {
		return time.Time{}, nil
	}
	var kinds []taskKind
	for _, sl := range slots {
		if !sl.at.IsZero() && sl.at.Equal(earliest) {
			kinds = append(kinds, sl.kind)
		}
	}
	return earliest, kinds
}

// Run 主循环，阻塞直到 ctx 取消。
func (s *Scheduler) Run(ctx context.Context) {
	// 余额定期刷新独立于整点排程：间隔驱动，且六类整点任务全禁用时也要照常跑，
	// 故不并入下面的 nextWake 循环，单独起一个 ticker goroutine。
	if s.cfg.CreditRefreshInterval > 0 {
		go s.runCreditRefresh(ctx)
	}
	for {
		next, kinds := s.nextWake(time.Now())
		if next.IsZero() {
			// 六类任务全部禁用：不空转，只等退出信号。
			<-ctx.Done()
			return
		}
		timer := time.NewTimer(time.Until(next))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			// 到点任务在排程时确定（不依赖唤醒时刻的小时数），迟到唤醒也不会漏跑。
			for _, k := range kinds {
				s.dispatch(k)
			}
		}
	}
}

// runCreditRefresh 按 CreditRefreshInterval 定期刷新余额，ctx 取消即退出。
// ticker 的首个时点在间隔之后，故启动瞬间不抢签到/keepalive 的刷新（此刻 credits
// 刚由落盘恢复，也没有抢的必要）。
func (s *Scheduler) runCreditRefresh(ctx context.Context) {
	t := time.NewTicker(s.cfg.CreditRefreshInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.RunCreditRefreshNow()
		}
	}
}

// RunCreditRefreshNow 查一遍所有账号的上游余额并写回池子。
//
// 刻意用 Pool.SetCredits 而非签到那条 ReenableIfCredits：后者语义是「签到解冻」，
// 会把冷却（until/coolKind/softStreak）一并清掉；定期刷新余额只是读数更新，
// 不该让一个正在 429 软冷却或熔断中的账号因"查到余额还有"就被凭空放回池子。
// 解冻仍然只归签到管（见 CheckinAll）。
//
// 单账号失败只记日志、继续下一个：余额查不到不是账号故障，不该中断整轮刷新，
// 更不该影响账号状态（保持上一次读数，交由签到/keepalive 去修 token）。
func (s *Scheduler) RunCreditRefreshNow() {
	statuses := s.cfg.Pool.List()
	var okN, failN, skipN int
	var total int64
	// fresh 只装本轮真拿到的读数：余额告警必须建立在「刚刚上游确认过」的值上，
	// 否则刚部署/刚重启（credits 还是 state.json 里的 0 或缺省）会立刻误报。
	fresh := make(map[string]int64, len(statuses))
	for _, st := range statuses {
		if st.Disabled {
			skipN++
			continue
		}
		a := s.cfg.Pool.AuthByUID(st.UID)
		if a == nil || a.AccessToken == "" {
			skipN++
			continue
		}
		// access token 已过期就不打上游：必然 401，每轮刷屏只会掩盖真问题。
		// 补票归签到/keepalive（它们先 RefreshToken 再查），本刷新只管读数。
		if a.NeedsRefresh(0) {
			skipN++
			continue
		}
		remain, err := s.cfg.Upstream.UserResource(a)
		if err != nil {
			log.Printf("credit-refresh %s: %v", logfmt.UID8(st.UID), err)
			failN++
			continue
		}
		s.cfg.Pool.SetCredits(st.UID, remain)
		fresh[st.UID] = remain
		total += remain
		okN++
		// 账号间限速：最后一号之后不必再等。
		if okN+failN < len(statuses) {
			time.Sleep(creditRefreshAccountGap)
		}
	}
	log.Printf("credit refresh done: total=%d ok=%d fail=%d skipped=%d remain=%d",
		len(statuses), okN, failN, skipN, total)
	s.checkHealthAlerts(statuses, fresh, okN, failN)
}

// checkHealthAlerts 检查需要人介入的状态并推送：余额查询整体失败 / 余额低 / 池子停摆。
//
// 挂在余额刷新那一轮 tick 上而非 handler 热路径：既拿到了刚刷新的读数，又完全不碰
// 请求路径（推送是旁路，不该出现在 chat 的延迟里）。去重靠 Notify 的冷却，
// key 的粒度决定重推频率：持续状态用固定 key（冷却后仍在异常则重推一次），
// 偶发事件在调用方带自然日（见 CheckinAll）。
//
// 只对 fresh（本轮上游确认过的读数）判余额，理由见 RunCreditRefreshNow 的注释。
func (s *Scheduler) checkHealthAlerts(statuses []pool.Status, fresh map[string]int64, okN, failN int) {
	if !s.cfg.Notify.Enabled() {
		return
	}
	// 全批查询失败 = 网络/上游不通（2026-09-27 的 DNS 事故就是这个形态：签到与
	// 余额查询同时全灭）。不与签到失败重复告警，但两者覆盖的时机不同（签到每天 2 次、
	// 这里每轮都查），所以各自保留。
	if okN == 0 && failN > 0 {
		s.cfg.Notify.Send("credit_refresh_down",
			"🚨 wb2api 余额查询全部失败",
			fmt.Sprintf("时间：%s<br><b>%d 个账号全部查不到余额</b><br>通常是网络/DNS 或上游故障；签到与余额刷新都会同时失效。",
				nowCST(), failN))
	}
	var lowTotal int64
	var parts []string
	for _, st := range statuses {
		credits, ok := fresh[st.UID]
		if !ok {
			continue
		}
		lowTotal += credits
		parts = append(parts, fmt.Sprintf("%s %d", displayName(st), credits))
		if s.cfg.BalanceLowPerAccount > 0 && credits < s.cfg.BalanceLowPerAccount {
			s.cfg.Notify.Send("balance_low:uid:"+st.UID,
				fmt.Sprintf("⚠️ wb2api 余额告警：%s 剩 %d", displayName(st), credits),
				fmt.Sprintf("账号：<b>%s</b>（%s）<br>余额：<b>%d</b>（阈值 %d）<br>时间：%s<br>余额耗尽后该号会被硬冷却到次日 04:00。",
					displayName(st), logfmt.UID8(st.UID), credits, s.cfg.BalanceLowPerAccount, nowCST()))
		}
	}
	if s.cfg.BalanceLowTotal > 0 && len(fresh) > 0 && lowTotal < s.cfg.BalanceLowTotal {
		s.cfg.Notify.Send("balance_low:total",
			fmt.Sprintf("⚠️ wb2api 全池余额偏低：%d", lowTotal),
			fmt.Sprintf("全池剩余：<b>%d</b>（阈值 %d）<br>分账号：%s<br>时间：%s",
				lowTotal, s.cfg.BalanceLowTotal, strings.Join(parts, " / "), nowCST()))
	}
	// 池子停摆：一个能服务的号都没有 = chat 请求会直接 503，属于必须立刻知道的事。
	if total, healthy, cooling, disabled, _ := s.cfg.Pool.CountsDetailed(); healthy == 0 {
		s.cfg.Notify.Send("pool_down",
			"🚨 wb2api 池子无法服务",
			fmt.Sprintf("时间：%s<br><b>healthy=0</b>（total=%d cooling=%d disabled=%d）<br>所有账号冷却或禁用，chat 请求将直接返回 503。",
				nowCST(), total, cooling, disabled))
	}
}

// nowCST 当前 CST 时刻（推送文案里的时间戳，与上游重置口径一致）。
func nowCST() string { return time.Now().In(cstZone).Format("2006-01-02 15:04:05") }

// displayName 账号展示名：昵称优先，空则回落 uid 前 8 位。
func displayName(st pool.Status) string {
	if st.Nickname != "" {
		return st.Nickname
	}
	return logfmt.UID8(st.UID)
}

// dispatch 按任务类型分发到对应执行函数。脚本类（school/cat）失败只记 WARN、
// 不影响其余任务继续执行（与现有各任务"单账号失败不阻断遍历"同口径）。
func (s *Scheduler) dispatch(k taskKind) {
	switch k {
	case taskCheckin:
		s.RunCheckinNow()
	case taskTravel:
		s.RunTravelNow()
	case taskActivity:
		s.RunActivityNow()
	case taskKeepalive:
		s.RunKeepaliveNow()
	case taskSchool:
		s.RunSchoolNow()
	case taskCat:
		s.RunCatNow()
	}
}

// RunCheckinNow 定时触发的立即签到：逐账号结果由 CheckinAll 记日志，此处只兜住"撞车跳过"。
func (s *Scheduler) RunCheckinNow() {
	if _, err := s.CheckinAll(); err != nil {
		log.Printf("scheduled checkin skipped: %v", err)
	}
}

// CheckinAll 全量签到：按需刷新 token → daily-checkin → 查余额 → 解冻冷却账号。
// 冷却中的账号也参与（签到就是为了解冻它们）；禁用的跳过。
// 同一时刻只允许一次签到在跑，重复调用返回 ErrBusy（防止手动触发与定时撞车重复打上游）。
//
// session dead 走 Pool.NoteSessionDead 的**连续计数**语义（与 keepalive 一致）：
// 一次刷新失败不再立即杀号，连续 sessionDeadThreshold 次才禁用，刷新成功清计数。
func (s *Scheduler) CheckinAll() ([]CheckinOutcome, error) {
	if !s.checkinMu.TryLock() {
		return nil, ErrBusy
	}
	defer s.checkinMu.Unlock()

	statuses := s.cfg.Pool.List()
	out := make([]CheckinOutcome, 0, len(statuses))
	var okN, alreadyN, failN, skipN int
	for _, st := range statuses {
		oc := CheckinOutcome{UID: st.UID, Nickname: st.Nickname}
		if st.Disabled {
			oc.Status, oc.Detail = CheckinSkipped, "disabled"
			skipN++
			out = append(out, oc)
			continue
		}
		a := s.cfg.Pool.AuthByUID(st.UID)
		if a == nil || a.RefreshToken == "" {
			oc.Status, oc.Detail = CheckinSkipped, "no credentials"
			skipN++
			out = append(out, oc)
			continue
		}
		// D4 门控：realm=global 账号无签到体系/任务中心，直接跳过（不发起任何上游调用，避免风控）。
		// 经 auth.Realm() 统一判定：逃生门（global.enabled=false）下 global 账号被降级为 cn、
		// 按 CN 处理——这是 D5 逃生门的刻意语义（纯 CN 部署锁死一切 global），与引用处一致。
		if a.IsGlobal() {
			oc.Status, oc.Detail = CheckinSkipped, "global"
			skipN++
			out = append(out, oc)
			continue
		}
		// 停机跨过 token 有效期（关机过夜/容器长期停跑）时先补一次刷新，否则签到必然 401 白跑。
		if a.NeedsRefresh(checkinRefreshSkew) {
			if err := s.cfg.Upstream.RefreshToken(a); err != nil {
				log.Printf("checkin %s refresh: %v", logfmt.UID8(st.UID), err)
				var ue *upstream.Error
				if errors.As(err, &ue) && ue.Kind == upstream.ErrSessionDead {
					if s.cfg.Pool.NoteSessionDead(st.UID) {
						log.Printf("WARN: checkin %s: 连续 %d 次 12153 session dead — 禁用", logfmt.UID8(st.UID), pool.SessionDeadThreshold())
					}
				}
				// 刷新只是"提前补票"：token 若仍有效，继续照常签到（否则刷新接口抖动
				// 会让本可成功的签到被白白跳过）；真正过期才判定失败。
				if a.NeedsRefresh(0) {
					oc.Status, oc.Detail = CheckinFail, "refresh: "+err.Error()
					failN++
					out = append(out, oc)
					continue
				}
			} else {
				a.BackfillRealm() // 老 auth 空 realm → 落盘前补标识（幂等：已有不动）
				if err := a.SaveAtomic(); err != nil {
					// 刷新成功但落盘失败：重启会用旧 token，必须暴露。
					log.Printf("checkin %s save: %v", logfmt.UID8(st.UID), err)
				}
			}
		}
		// 签到返回错误（含"今天已签到"）也继续查余额：余额恢复即可解冻账号。
		if err := s.cfg.Upstream.DailyCheckin(a); err != nil {
			if upstream.IsAlreadyCheckin(err) {
				// "今天已签到"是幂等成功，不是错误：不填 detail，免得回执里
				// 出现一整段 400 报文、被误读成签到失败。
				oc.Status = CheckinAlready
			} else {
				oc.Status = CheckinFail
				oc.Detail = err.Error()
				log.Printf("checkin %s: %v", logfmt.UID8(st.UID), err)
			}
		} else {
			oc.Status = CheckinOK
		}
		remain, err := s.cfg.Upstream.UserResource(a)
		if err != nil {
			log.Printf("user-resource %s: %v", logfmt.UID8(st.UID), err)
			oc.Status = CheckinFail
			oc.Detail = joinDetail(oc.Detail, "resource: "+err.Error())
			failN++
			out = append(out, oc)
			continue
		}
		s.cfg.Pool.ReenableIfCredits(st.UID, remain)
		oc.Credits = &remain
		switch oc.Status {
		case CheckinOK:
			okN++
		case CheckinAlready:
			alreadyN++
		default:
			failN++
		}
		out = append(out, oc)
	}
	log.Printf("checkin done: total=%d ok=%d already=%d fail=%d skipped=%d",
		len(statuses), okN, alreadyN, failN, skipN)
	s.alertCheckinFail(out, failN)
	return out, nil
}

// alertCheckinFail 签到有失败就推一条。
//
// key 带 CST 自然日 → 每天至多一条：签到每天只跑 2 次，「今天这趟没成」用日期做 key
// 比冷却窗口更贴合语义，且第二天失败仍会照常推。
//
// 为什么值得单独告警：签到失败 → 当天没有回补 → 余额只减不增。2026-09-27 21:00 就是
// 四号全因 DNS 失败，没有重试、没有告警，次日余额继续往下走且无人知晓。
func (s *Scheduler) alertCheckinFail(out []CheckinOutcome, failN int) {
	if failN == 0 || !s.cfg.Notify.Enabled() {
		return
	}
	var lines []string
	for _, oc := range out {
		if oc.Status != CheckinFail {
			continue
		}
		name := oc.Nickname
		if name == "" {
			name = logfmt.UID8(oc.UID)
		}
		lines = append(lines, fmt.Sprintf("<b>%s</b> %s", name, truncRunes(oc.Detail, 80)))
	}
	s.cfg.Notify.Send("checkin_fail:"+travelDay(time.Now()),
		fmt.Sprintf("⚠️ wb2api 签到失败 %d/%d", failN, len(out)),
		fmt.Sprintf("时间：%s<br>%s<br>签到失败的当天没有回补，余额只减不增。",
			nowCST(), strings.Join(lines, "<br>")))
}

// truncRunes 按字符（非字节）截断，避免把多字节字切坏。
func truncRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

// joinDetail 拼接多段原因，避免后一段覆盖前一段的失败信息。
func joinDetail(existing, add string) string {
	if existing == "" {
		return add
	}
	return existing + "; " + add
}

// RunActivityNow 立即对池内所有可用账号执行对话活跃上报。
// 禁用账号跳过；无 AccessToken 的跳过；账号间限速 activityAccountDelay。
// CN 与 global 账号**都上报**（PR #45 实测国际版 /v2/report 可用）；单账号失败
// 只记 WARN 不影响遍历。
//
// 每号上报 N 条（ActivityReportCount，默认 5）：N 条共用同一 conversationId
// （wb2api-<ms>），模拟同一会话内 N 轮对话——这是领养猫（buddy/first）对话量
// 门槛的实测刷法（chat_5 前置需 5 次对话）。requestId 各条独立（同会话多轮）。
// 账号内 N 条之间间隔 activityReportGap（1.5s）避免秒发触发风控。
//
// 0/缺省 ActivityReportCount = 1 条，兼容旧行为（仅点亮连登 + 解锁 first_buddy）。
//
// 上报成功后：① streak 自检（回读连登，发现「200 但静默丢弃」）；
// ② 无猫账号立即重试领养（travelAdoptForce）——对话量刚补满的新状态，不算重试，
// 豁免 adoptTriedToday 当日防抖（旅行排程 09 点已领养过且 skip，10 点上报补满后
// 不能依赖下一轮旅行领养，就地闭环）。
func (s *Scheduler) RunActivityNow() {
	count := s.cfg.ActivityReportCount
	first := true
	for _, st := range s.cfg.Pool.List() {
		if st.Disabled {
			continue
		}
		a := s.cfg.Pool.AuthByUID(st.UID)
		if a == nil || a.AccessToken == "" {
			continue
		}
		// global 账号同样上报（PR #45 实测国际版 /v2/report 在 workbuddy.ai 上 code=0 OK，
		// 点亮连登）；realmBase 路由/头由 upstream.billingJSON/BillingHeaders 按 realm 切。
		// 单账号失败只记 WARN 不影响遍历（下方 report err → break 该号 → continue 下号）。
		if !first {
			time.Sleep(activityAccountDelay)
		}
		first = false
		// N 条共用同一 conversationId（同会话），requestId 各自独立（每条一个）。
		cid := fmt.Sprintf("wb2api-%d", time.Now().UnixMilli())
		ok := 0
		for i := 1; i <= count; i++ {
			rid := fmt.Sprintf("%s-r%d", cid, i)
			if err := s.cfg.Upstream.ReportChatActivity(a, cid, rid); err != nil {
				log.Printf("activity %s: report %d/%d: %v", logfmt.UID8(a.UID), i, count, err)
				break // 本号上报失败：不再续发，streak 自检无意义
			}
			log.Printf("activity %s: report %d/%d ok", logfmt.UID8(a.UID), i, count)
			ok++
			if i < count {
				time.Sleep(activityReportGap) // 账号内 5 条之间间隔，避免秒发风控
			}
		}
		if ok < count {
			continue // N 条未发满：streak 自检与领养均无意义，下个账号
		}
		s.checkActivityStreak(a) // N 条全发满 → 回读 streak 自检（只留结论行）
		s.travelAdoptForce(a)    // 无猫账号对话量刚补满 → 立即重试领养（豁免防抖）
	}
}

// checkActivityStreak 上报成功后回读连登天数（只读 oracle，发现静默失败）。
// 背景：REPORT-active-map.md §2 实测「上报 200 但静默丢弃」（缺 userId 时 progress 不动），
// 上报 200 ≠ streak 计分——需要回读验证闭环。
// 异常检测口径：days==0 → warn（report OK but streak.days=0 (silent drop?)）；
// GET 失败 → warn 但不影响主流程（上报本身已成功，按天幂等，不做重试）。
// 日志每号一行、一眼可 grep：`activity %s: streak days=%d`（成功也打，方便对账）。
// 返回 true 表示「上报 OK 但 streak 可疑」（days==0 或回读失败），供测试断言。
func (s *Scheduler) checkActivityStreak(a *auth.Auth) bool {
	days, err := s.cfg.Upstream.GrowthStreak(a)
	if err != nil {
		log.Printf("WARN: activity %s: streak check failed (report OK): %v", logfmt.UID8(a.UID), err)
		return true
	}
	if days == 0 {
		log.Printf("WARN: activity %s: report OK but streak.days=0 (silent drop?)", logfmt.UID8(a.UID))
		return true
	}
	log.Printf("activity %s: streak days=%d", logfmt.UID8(a.UID), days)
	return false
}

// RunKeepaliveNow 立即对所有账号刷新 token；session 死亡的自动禁用。
// 12153 禁用走 Pool.NoteSessionDead 的**连续计数**语义：一次刷新失败不再立即杀号，
// 连续 sessionDeadThreshold 次（3 次）才禁用（P0-1：13 个 disabled 号全是历史误判）。
// 刷新成功 → ClearSessionDead 清计数（错误判定的账号有复活路径）。
func (s *Scheduler) RunKeepaliveNow() {
	for _, st := range s.cfg.Pool.List() {
		if st.Disabled {
			continue
		}
		a := s.cfg.Pool.AuthByUID(st.UID)
		if a == nil || a.RefreshToken == "" {
			continue
		}
		if err := s.cfg.Upstream.RefreshToken(a); err != nil {
			log.Printf("keepalive %s: %v", logfmt.UID8(st.UID), err)
			var ue *upstream.Error
			if errors.As(err, &ue) && ue.Kind == upstream.ErrSessionDead {
				if s.cfg.Pool.NoteSessionDead(st.UID) {
					log.Printf("WARN: keepalive %s: 连续 %d 次 12153 session dead — 禁用", logfmt.UID8(st.UID), pool.SessionDeadThreshold())
				}
			}
			continue
		}
		s.cfg.Pool.ClearSessionDead(st.UID) // 刷新成功清误判计数，失败不该累计
		a.BackfillRealm()                   // 老 auth 空 realm → 落盘前补标识（幂等：已有不动）
		if err := a.SaveAtomic(); err != nil {
			log.Printf("keepalive %s save: %v", logfmt.UID8(st.UID), err)
		}
	}
}
