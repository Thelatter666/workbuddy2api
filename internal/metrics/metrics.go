// Package metrics 内存版请求统计采集器：按小时/天两级定长时间环记录
// 请求数、tokens、credit 消耗、成功率与 TTFB，供 /dashboard 统计面板；
// 另有一份跨年的紧凑日台账（LedgerDay）承载「长期用量 + 积分消耗」视图。
//
// 设计取向（对齐本仓库 stdlib 优先）：
//   - 零外部依赖，仅用环形定长分桶，内存占用恒定、不随运行时长增长；
//   - 槽位用「周期序号」自校验，过期槽在下次写入时就地重置，无需后台 GC；
//   - 热数据（小时环 7 天 / 日环 30 天）+ 冷数据（台账 3 年）分层：热数据 5s 落盘，
//     台账每天最多写一次，长期保留年限不放大落盘开销。
//
// 记录来源是 server.chatStat.done() 的唯一出口，故每个 chat 请求恰好记一次。
package metrics

import (
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// HourSlots 小时环槽数：7 天 × 24 小时，用于趋势折线。
	HourSlots = 168
	// DaySlots 日环槽数：30 天，用于按日对比与维度下钻。
	DaySlots = 30
	// LedgerDays 长期日台账保留天数（3 年）。日环只有 30 槽，装不下「长期监控积分
	// 消耗」需要的跨度；日环里已结束的日子会被沉淀成紧凑台账行（见 absorbLedgerLocked），
	// 台账一行只留面板长期图表要用的量，故 3 年的体积也就几百 KB，且每天最多写一次盘。
	LedgerDays = 1095
)

// LedgerDim 台账维度的紧凑形态：长期图表只需要请求数与 credit，不保留状态码/TTFB
// 这类只在短期窗口里有意义的值——台账要留 3 年，字段省一半体积就省一半。
type LedgerDim struct {
	Requests int64   `json:"requests,omitempty"`
	Credit   float64 `json:"credit,omitempty"`
}

// LedgerDay 一个「已结束日」的长期台账行。Index 是日周期序号（UTC+8 切日，同 dayOffset）。
type LedgerDay struct {
	Index            int64                `json:"index"`
	Requests         int64                `json:"requests,omitempty"`
	OK               int64                `json:"ok,omitempty"`
	Fail             int64                `json:"fail,omitempty"`
	PromptTokens     int64                `json:"prompt_tokens,omitempty"`
	CompletionTokens int64                `json:"completion_tokens,omitempty"`
	Tokens           int64                `json:"tokens,omitempty"`
	Credit           float64              `json:"credit,omitempty"`
	ByUID            map[string]LedgerDim `json:"by_uid,omitempty"`
	ByModel          map[string]LedgerDim `json:"by_model,omitempty"`
	// Balances 当日收盘余额（uid → 剩余积分）。只有采到过快照的账号才有键；
	// 这是唯一能反映「签到奖励 / 人工充值」的口径——逐请求 credit 只看得见消耗。
	Balances map[string]int64 `json:"balances,omitempty"`
}

// dayOffset 日环按 UTC+8 切日：与上游 429 重置文案（UTC+8）及部署 TZ=Asia/Shanghai 同口径，
// 避免跨零点时把同一天算成两天。
const dayOffset = int64(8 * 60 * 60)

// cstZone 日环切日时区。日期的「标签」（long_term.dates / CSV）必须按它输出：
// startOf 返回的是切日瞬间（UTC 时刻，即前一天 16:00Z），直接 Format 会得到前一天的日期，
// 让整个面板与导出的 CSV 整体错一天。数值口径一直是对的，错的只是标签。
var cstZone = time.FixedZone("CST", int(dayOffset))

// Dim 一组计数器（标量累加 + 维度明细共用）。
type Dim struct {
	Requests         int64   `json:"requests"`
	OK               int64   `json:"ok"`
	Fail             int64   `json:"fail"`
	PromptTokens     int64   `json:"prompt_tokens"`
	CompletionTokens int64   `json:"completion_tokens"`
	Tokens           int64   `json:"tokens"` // prompt + completion
	Credit           float64 `json:"credit"`
}

// Sample 单个已结束 chat 请求的统计切片，由 server.chatStat 投影而来。
type Sample struct {
	Start        time.Time // 请求进入 handler 的时刻（决定归属周期）
	Model        string
	Mode         string // "stream" | "sync"
	UID          string // 完整 uid
	Status       int    // HTTP 状态（0 视作失败）
	Tokens       int    // completion tokens；<0 表示 usage 缺失
	PromptTokens int
	Credit       float64
	HasCredit    bool // usage.credit 是否显式出现（缺失 ≠ 0）
	TTFB         time.Duration
}

// bucket 单个周期槽位。index 记录该槽当前承载的周期序号，用于识别过期槽。
type bucket struct {
	index int64 // -1 = 空槽（life 累计器恒 -1，永不过期）
	Dim
	ttfbSumNs int64
	ttfbCount int64
	ttfbMaxNs int64
	byUID     map[string]*Dim
	byModel   map[string]*Dim
	byStatus  map[string]int64
	withDims  bool
	// balances 当日采到的账号余额快照（uid → 剩余积分）。日环专有：同一周期内覆盖写，
	// 于是留在槽里的就是「当日收盘余额」。小时环不采集（余额是日粒度指标）。
	balances map[string]int64
}

// add 累加一个样本的标量计数。
func (b *bucket) add(s Sample) {
	b.Requests++
	if s.Status >= 200 && s.Status < 300 {
		b.OK++
	} else {
		b.Fail++
	}
	// Tokens 允许为 -1（usage 缺失哨兵），必须先钳 0 再合计，否则会反噬 prompt 计数。
	prompt, comp := nonNeg(s.PromptTokens), nonNeg(s.Tokens)
	b.PromptTokens += int64(prompt)
	b.CompletionTokens += int64(comp)
	b.Tokens += int64(prompt + comp)
	if s.HasCredit {
		b.Credit += s.Credit
	}
	if s.TTFB > 0 {
		b.ttfbSumNs += int64(s.TTFB)
		b.ttfbCount++
		if int64(s.TTFB) > b.ttfbMaxNs {
			b.ttfbMaxNs = int64(s.TTFB)
		}
	}
}

// addDims 累加维度明细（仅日环与 life 启用，小时环不需要）。
func (b *bucket) addDims(s Sample) {
	if !b.withDims {
		return
	}
	if b.byUID == nil {
		b.byUID = map[string]*Dim{}
	}
	if b.byModel == nil {
		b.byModel = map[string]*Dim{}
	}
	if b.byStatus == nil {
		b.byStatus = map[string]int64{}
	}
	dimOf := func(m map[string]*Dim, key string) *Dim {
		d := m[key]
		if d == nil {
			d = &Dim{}
			m[key] = d
		}
		return d
	}
	// 维度内只累加标量（不递归维度），故直接复用 add 的标量口径。
	for _, d := range []*Dim{dimOf(b.byUID, s.UID), dimOf(b.byModel, s.Model)} {
		d.addScalar(s)
	}
	b.byStatus[statusKey(s.Status)]++
}

// addScalar 与 bucket.add 同口径的标量累加，供维度明细复用（无 TTFB/子维度）。
func (d *Dim) addScalar(s Sample) {
	d.Requests++
	if s.Status >= 200 && s.Status < 300 {
		d.OK++
	} else {
		d.Fail++
	}
	prompt, comp := nonNeg(s.PromptTokens), nonNeg(s.Tokens)
	d.PromptTokens += int64(prompt)
	d.CompletionTokens += int64(comp)
	d.Tokens += int64(prompt + comp)
	if s.HasCredit {
		d.Credit += s.Credit
	}
}

func statusKey(code int) string {
	if code == 0 {
		return "unknown"
	}
	return strconv.Itoa(code)
}

// nonNeg 把 usage 缺失哨兵（-1）钳到 0；正常 token 计数原样返回。
func nonNeg(n int) int {
	if n < 0 {
		return 0
	}
	return n
}

// ring 定长周期环。slot = idx % len(slots)，idx = (unix + offset) / periodSec。
type ring struct {
	slots     []bucket
	periodSec int64
	offset    int64
}

func newRing(n int, periodSec, offset int64, withDims bool) *ring {
	r := &ring{slots: make([]bucket, n), periodSec: periodSec, offset: offset}
	for i := range r.slots {
		r.slots[i].index = -1
		r.slots[i].withDims = withDims
	}
	return r
}

func (r *ring) idxOf(t time.Time) int64 {
	return (t.Unix() + r.offset) / r.periodSec
}

// at 返回 t 所属槽位；若槽位承载的是更早周期，则就地重置（过期即回收）。
func (r *ring) at(t time.Time) *bucket {
	idx := r.idxOf(t)
	n := int64(len(r.slots))
	b := &r.slots[((idx%n)+n)%n]
	if b.index != idx {
		withDims := b.withDims
		*b = bucket{index: idx, withDims: withDims} // 丢弃旧 map，惰性重建
	}
	return b
}

// startOf 返回槽位 idx 对应的周期起点（UTC 时刻）。
func (r *ring) startOf(idx int64) time.Time {
	return time.Unix(idx*r.periodSec-r.offset, 0).UTC()
}

// Point 单个周期的统计快照（JSON 输出用，时间字段已格式化）。
type Point struct {
	Ts               string  `json:"ts"`
	Requests         int64   `json:"requests"`
	OK               int64   `json:"ok"`
	Fail             int64   `json:"fail"`
	PromptTokens     int64   `json:"prompt_tokens"`
	CompletionTokens int64   `json:"completion_tokens"`
	Tokens           int64   `json:"tokens"`
	Credit           float64 `json:"credit"`
	TTFBAvgMs        float64 `json:"ttfb_avg_ms"`
	TTFBMaxMs        int64   `json:"ttfb_max_ms"`
}

// DayPoint 在 Point 基础上带维度明细（按账号/模型/状态码下钻）。
type DayPoint struct {
	Point
	ByUID    map[string]Dim   `json:"by_uid,omitempty"`
	ByModel  map[string]Dim   `json:"by_model,omitempty"`
	ByStatus map[string]int64 `json:"by_status,omitempty"`
}

func pointOf(b *bucket, ts string) Point {
	p := Point{
		Ts:               ts,
		Requests:         b.Requests,
		OK:               b.OK,
		Fail:             b.Fail,
		PromptTokens:     b.PromptTokens,
		CompletionTokens: b.CompletionTokens,
		Tokens:           b.Tokens,
		Credit:           b.Credit,
		TTFBMaxMs:        b.ttfbMaxNs / int64(time.Millisecond),
	}
	if b.ttfbCount > 0 {
		p.TTFBAvgMs = float64(b.ttfbSumNs) / float64(b.ttfbCount) / float64(time.Millisecond)
	}
	return p
}

// Collector 采集器：小时环 + 日环 + 进程累计（life）+ 跨年日台账（ledger）。
//
// stateFp 非空时启用落盘（见 persist.go）：热数据（hours/days）与台账（ledgerFp）
// 分开两个文件——热数据 5s 一次、台账每天至多一次，避免长期保留反过来放大写盘量。
// life 保持重启清零——面板「本进程请求」标签的语义因此不变。
type Collector struct {
	mu        sync.Mutex
	hours     *ring
	days      *ring
	life      *bucket
	startedAt time.Time

	// ledger 长期日台账：按 Index 升序，容量 LedgerDays。只沉淀「已结束日」，
	// 今天始终由日环实时提供（避免同一天两处写导致口径打架）。
	ledger    []*LedgerDay
	ledgerIdx map[int64]*LedgerDay

	// balanceSrc 账号余额来源（通常为 pool.List() 的投影）。nil = 不采集余额。
	balanceSrc func() map[string]int64

	// 落盘相关（stateFp/ledgerFp=="" 时相应路径为空操作）。
	stateFp     string
	ledgerFp    string
	dirty       atomic.Bool // 热数据有变更待落盘
	ledgerDirty atomic.Bool // 台账有变更待落盘（通常每天一次）
	// persistFails 连续落盘失败计数（仅 saveLocked 在持锁下读写，无需 atomic）。
	persistFails int
}

// New 构造采集器（空历史，纯内存，不落盘）。
func New() *Collector {
	life := &bucket{index: -1, withDims: true}
	return &Collector{
		hours:     newRing(HourSlots, 3600, 0, false),
		days:      newRing(DaySlots, 86400, dayOffset, true),
		life:      life,
		startedAt: time.Now(),
		ledgerIdx: map[int64]*LedgerDay{},
	}
}

// Record 记录一个已结束的 chat 请求。
func (c *Collector) Record(s Sample) {
	c.mu.Lock()
	defer c.mu.Unlock()
	h := c.hours.at(s.Start)
	h.add(s)
	d := c.days.at(s.Start)
	d.add(s)
	d.addDims(s)
	c.life.add(s)
	c.life.addDims(s)
	c.dirty.Store(true) // 有待落盘变更（stateFp=="" 时 Flush 空操作，代价仅一次原子写）
}

// Reset 清空全部历史（仅测试用：隔离用例，对齐 chatLogEnabled/resetModelsCache 惯例）。
// 注意：不可用 *c = *New() 就地覆盖——那会把 mu 一并替换成副本，使 defer Unlock 解锁
// 一个未加锁的 mutex 而 panic（fatal error: unlock of unlocked mutex）。逐个换掉受 mu
// 保护的字段，mutex 自身保持不变。
func (c *Collector) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hours = newRing(HourSlots, 3600, 0, false)
	c.days = newRing(DaySlots, 86400, dayOffset, true)
	c.life = &bucket{index: -1, withDims: true}
	c.ledger = nil
	c.ledgerIdx = map[int64]*LedgerDay{}
	c.startedAt = time.Now()
	c.dirty.Store(false)       // 空历史无需落盘
	c.ledgerDirty.Store(false) // 台账同理
}

// SetBalanceSource 注入账号余额来源（装配期调用一次）。src 返回 uid → 剩余积分；
// 返回空 map 视作「本次没采到」，不覆盖已有快照。
func (c *Collector) SetBalanceSource(src func() map[string]int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.balanceSrc = src
}

// PollBalances 主动采集一次余额快照（供 API 读取前刷新；落盘路径内部也会调）。
func (c *Collector) PollBalances() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pollBalancesLocked()
}

// pollBalancesLocked 把当前余额写进「今日」槽位（同日覆盖 = 收盘值）。
// 余额变化才置 dirty：静止时不该把台账/热数据文件反复刷盘。
func (c *Collector) pollBalancesLocked() {
	if c.balanceSrc == nil {
		return
	}
	cur := c.balanceSrc()
	if len(cur) == 0 {
		return
	}
	b := c.days.at(time.Now())
	if balancesEqual(b.balances, cur) {
		return
	}
	next := make(map[string]int64, len(cur))
	for k, v := range cur {
		next[k] = v
	}
	b.balances = next
	c.dirty.Store(true)
}

func balancesEqual(a, b map[string]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// ---- 长期日台账 ----

// absorbLedgerLocked 把日环里「已结束」的日子沉淀进长期台账。
//   - 今天不沉淀：当日始终由日环实时提供，同一天两处写必然打架；
//   - 已存在的行做覆盖更新：跨零点才结束的请求（Sample.Start 仍属昨天）会在日环里
//     补写昨天的槽，台账要跟着走。日环滚出 30 天槽后，台账留下的即最后一次同步值。
func (c *Collector) absorbLedgerLocked() {
	today := c.days.idxOf(time.Now())
	for i := range c.days.slots {
		b := &c.days.slots[i]
		if b.index < 0 || b.index >= today {
			continue
		}
		row := ledgerRowOf(b)
		if old := c.ledgerIdx[row.Index]; old != nil {
			if ledgerEqual(old, row) {
				continue
			}
			*old = *row
		} else {
			c.insertLedgerLocked(row)
		}
		c.ledgerDirty.Store(true)
	}
}

// insertLedgerLocked 按 Index 升序插入台账行并维持容量上限（超限丢最旧）。
func (c *Collector) insertLedgerLocked(row *LedgerDay) {
	c.ledgerIdx[row.Index] = row
	pos := len(c.ledger)
	for pos > 0 && c.ledger[pos-1].Index > row.Index {
		pos--
	}
	c.ledger = append(c.ledger, nil)
	copy(c.ledger[pos+1:], c.ledger[pos:])
	c.ledger[pos] = row
	if over := len(c.ledger) - LedgerDays; over > 0 {
		for _, old := range c.ledger[:over] {
			delete(c.ledgerIdx, old.Index)
		}
		c.ledger = append([]*LedgerDay(nil), c.ledger[over:]...)
	}
}

// ledgerRowOf 把日环槽位投影成台账行（只留长期图表要用的量）。
func ledgerRowOf(b *bucket) *LedgerDay {
	row := &LedgerDay{
		Index:            b.index,
		Requests:         b.Requests,
		OK:               b.OK,
		Fail:             b.Fail,
		PromptTokens:     b.PromptTokens,
		CompletionTokens: b.CompletionTokens,
		Tokens:           b.Tokens,
		Credit:           b.Credit,
	}
	if len(b.byUID) > 0 {
		row.ByUID = make(map[string]LedgerDim, len(b.byUID))
		for k, v := range b.byUID {
			row.ByUID[k] = LedgerDim{Requests: v.Requests, Credit: v.Credit}
		}
	}
	if len(b.byModel) > 0 {
		row.ByModel = make(map[string]LedgerDim, len(b.byModel))
		for k, v := range b.byModel {
			row.ByModel[k] = LedgerDim{Requests: v.Requests, Credit: v.Credit}
		}
	}
	if len(b.balances) > 0 {
		row.Balances = make(map[string]int64, len(b.balances))
		for k, v := range b.balances {
			row.Balances[k] = v
		}
	}
	return row
}

func ledgerEqual(a, b *LedgerDay) bool {
	if a.Requests != b.Requests || a.OK != b.OK || a.Fail != b.Fail ||
		a.PromptTokens != b.PromptTokens || a.CompletionTokens != b.CompletionTokens ||
		a.Tokens != b.Tokens || a.Credit != b.Credit {
		return false
	}
	if !ledgerDimsEqual(a.ByUID, b.ByUID) || !ledgerDimsEqual(a.ByModel, b.ByModel) {
		return false
	}
	return balancesEqual(a.Balances, b.Balances)
}

func ledgerDimsEqual(a, b map[string]LedgerDim) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		w, ok := b[k]
		if !ok || v != w {
			return false
		}
	}
	return true
}

// LongTermSeries 长期视图：列式平行数组（面板直接喂图表，比行式省一个数量级的体积）。
type LongTermSeries struct {
	Start        string               `json:"start"`    // 窗口首日（YYYY-MM-DD，UTC+8）
	Since        string               `json:"since"`    // 窗口内最早有数据的一天，"" = 全空
	Dates        []string             `json:"dates"`    // 连续日，含零值日
	Requests     []int64              `json:"requests"` // 按日请求数
	Tokens       []int64              `json:"tokens"`   // 按日 tokens
	Credit       []float64            `json:"credit"`   // 按日 credit 消耗
	Accounts     map[string]SeriesDim `json:"accounts,omitempty"`
	Models       map[string]SeriesDim `json:"models,omitempty"`
	CoverageDays int                  `json:"coverage_days"` // 窗口内有数据的天数
	LedgerDays   int                  `json:"ledger_days"`   // 台账保留上限（供面板做区间上限）
}

// SeriesDim 单维度平行数组。Balance 用指针数组：null 表示当日没采到快照，
// 与「余额真的是 0」区分开——曲线断点比假 0 值诚实。
type SeriesDim struct {
	Requests []int64   `json:"requests,omitempty"`
	Credit   []float64 `json:"credit,omitempty"`
	Balance  []*int64  `json:"balance,omitempty"`
}

// longTermLocked 组装窗口 [today-days+1, today] 的长期序列：台账打底、日环覆盖
// （日环更权威且含今天）。缺日补零，保证 Dates 连续——图表不该自己猜缺口。
func (c *Collector) longTermLocked(now time.Time, days int) LongTermSeries {
	if days <= 0 || days > LedgerDays {
		days = LedgerDays
	}
	today := c.days.idxOf(now)
	from := today - int64(days) + 1

	merged := make(map[int64]*LedgerDay, len(c.ledger)+DaySlots)
	for _, row := range c.ledger {
		if row.Index >= from && row.Index <= today {
			merged[row.Index] = row
		}
	}
	for i := range c.days.slots {
		if b := &c.days.slots[i]; b.index >= from && b.index <= today {
			merged[b.index] = ledgerRowOf(b)
		}
	}

	out := LongTermSeries{
		Dates:      make([]string, 0, days),
		Requests:   make([]int64, 0, days),
		Tokens:     make([]int64, 0, days),
		Credit:     make([]float64, 0, days),
		LedgerDays: LedgerDays,
	}
	rows := make([]*LedgerDay, 0, days)
	uids, models := map[string]bool{}, map[string]bool{}
	for idx := from; idx <= today; idx++ {
		day := c.days.startOf(idx).In(cstZone).Format("2006-01-02")
		out.Dates = append(out.Dates, day)
		r := merged[idx]
		if r == nil {
			out.Requests, out.Tokens, out.Credit = append(out.Requests, 0), append(out.Tokens, 0), append(out.Credit, 0)
			rows = append(rows, nil)
			continue
		}
		out.Requests, out.Tokens, out.Credit = append(out.Requests, r.Requests), append(out.Tokens, r.Tokens), append(out.Credit, r.Credit)
		if r.Requests > 0 || r.Credit > 0 || len(r.Balances) > 0 {
			out.CoverageDays++
			if out.Since == "" {
				out.Since = day
			}
		}
		for k := range r.ByUID {
			uids[k] = true
		}
		// 只有余额快照、当天没流量的账号也要进维度：否则余额曲线会缺账号。
		for k := range r.Balances {
			uids[k] = true
		}
		for k := range r.ByModel {
			models[k] = true
		}
		rows = append(rows, r)
	}
	out.Start = out.Dates[0]
	out.Accounts = seriesDims(rows, uids, func(r *LedgerDay) map[string]LedgerDim { return r.ByUID }, true)
	out.Models = seriesDims(rows, models, func(r *LedgerDay) map[string]LedgerDim { return r.ByModel }, false)
	return out
}

// seriesDims 把维度展开成平行数组；withBalance 时额外抽当日余额（只有账号维度有）。
func seriesDims(rows []*LedgerDay, keys map[string]bool, pick func(*LedgerDay) map[string]LedgerDim, withBalance bool) map[string]SeriesDim {
	if len(keys) == 0 {
		return nil
	}
	out := make(map[string]SeriesDim, len(keys))
	for k := range keys {
		d := SeriesDim{
			Requests: make([]int64, len(rows)),
			Credit:   make([]float64, len(rows)),
		}
		var bal []*int64
		for i, r := range rows {
			if r == nil {
				continue
			}
			if v, ok := pick(r)[k]; ok {
				d.Requests[i], d.Credit[i] = v.Requests, v.Credit
			}
			if withBalance {
				if v, ok := r.Balances[k]; ok {
					if bal == nil {
						bal = make([]*int64, len(rows))
					}
					b := v
					bal[i] = &b
				}
			}
		}
		d.Balance = bal
		out[k] = d
	}
	return out
}

// Snapshot 输出面板所需的全部统计（长期序列取满 LedgerDays）。
func (c *Collector) Snapshot() Snapshot { return c.SnapshotDays(LedgerDays) }

// SnapshotDays 输出面板统计，长期序列只覆盖最近 days 天（<=0 或超界按 LedgerDays）。
// 面板的区间切换据此缩短响应体：看 7 天没必要把 3 年的平行数组都传过去。
func (c *Collector) SnapshotDays(days int) Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	c.pollBalancesLocked() // 每次读取都刷新当日余额，别让「今日」停在上一 tick
	c.absorbLedgerLocked() // 顺手沉淀已结束日，保证不打开面板也在攒长期数据
	snap := Snapshot{
		GeneratedAt: now.UTC().Format(time.RFC3339),
		UptimeSec:   int64(now.Sub(c.startedAt).Seconds()),
		Total:       c.life.Dim,
		ByUID:       copyDims(c.life.byUID),
		ByModel:     copyDims(c.life.byModel),
		ByStatus:    copyStatus(c.life.byStatus),
	}
	for _, b := range c.hours.series(now) {
		snap.Hours = append(snap.Hours, pointOf(b, c.hours.startOf(b.index).Format(time.RFC3339)))
	}
	for _, b := range c.days.series(now) {
		snap.Days = append(snap.Days, DayPoint{
			Point:    pointOf(b, c.days.startOf(b.index).Format(time.RFC3339)),
			ByUID:    copyDims(b.byUID),
			ByModel:  copyDims(b.byModel),
			ByStatus: copyStatus(b.byStatus),
		})
	}
	snap.TopModels = rankModels(c.life.byModel, 10)
	snap.LongTerm = c.longTermLocked(now, days)
	return snap
}

// series 按时间升序返回最近 len(slots) 个周期槽位（空槽为零值，ts 仍正确）。
func (r *ring) series(now time.Time) []*bucket {
	n := int64(len(r.slots))
	cur := r.idxOf(now)
	out := make([]*bucket, 0, n)
	for i := n - 1; i >= 0; i-- {
		idx := cur - i
		b := &r.slots[((idx%n)+n)%n]
		if b.index != idx {
			out = append(out, &bucket{index: idx}) // 空槽：零计数 + 正确周期起点
			continue
		}
		out = append(out, b)
	}
	return out
}

func copyDims(m map[string]*Dim) map[string]Dim {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]Dim, len(m))
	for k, v := range m {
		out[k] = *v
	}
	return out
}

func copyStatus(m map[string]int64) map[string]int64 {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]int64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// ModelStat 模型维度排行项。
type ModelStat struct {
	Model    string  `json:"model"`
	Requests int64   `json:"requests"`
	Tokens   int64   `json:"tokens"`
	Credit   float64 `json:"credit"`
	Fail     int64   `json:"fail"`
}

// rankModels 按请求数降序取前 limit 个模型。
func rankModels(m map[string]*Dim, limit int) []ModelStat {
	out := make([]ModelStat, 0, len(m))
	for k, v := range m {
		out = append(out, ModelStat{Model: k, Requests: v.Requests, Tokens: v.Tokens, Credit: v.Credit, Fail: v.Fail})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Requests != out[j].Requests {
			return out[i].Requests > out[j].Requests
		}
		return out[i].Model < out[j].Model
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}

// Snapshot 面板数据总览。
type Snapshot struct {
	GeneratedAt string           `json:"generated_at"`
	UptimeSec   int64            `json:"uptime_sec"`
	Total       Dim              `json:"total"` // 进程累计（重启清零）
	ByUID       map[string]Dim   `json:"by_uid,omitempty"`
	ByModel     map[string]Dim   `json:"by_model,omitempty"`
	ByStatus    map[string]int64 `json:"by_status,omitempty"`
	Hours       []Point          `json:"hours"` // 168 槽，时间升序
	Days        []DayPoint       `json:"days"`  // 30 槽，时间升序
	TopModels   []ModelStat      `json:"top_models"`
	LongTerm    LongTermSeries   `json:"long_term"` // 长期日序列（含积分与余额）
}
