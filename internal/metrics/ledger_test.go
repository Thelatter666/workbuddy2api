// ledger_test.go 长期日台账与余额快照的用例：日环溢出沉淀、跨重启回放、
// 窗口裁剪与余额指针数组的 null 语义。
package metrics

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// sampleAt 造一个落在 t 时刻的样本（Start 决定归属周期）。
func sampleAt(t time.Time, uid, model string, tokens int, credit float64) Sample {
	return Sample{
		Start:        t,
		Model:        model,
		UID:          uid,
		Status:       200,
		Tokens:       tokens,
		PromptTokens: 100,
		Credit:       credit,
		HasCredit:    true,
		TTFB:         10 * time.Millisecond,
	}
}

// TestLedgerAbsorbsCompletedDays 已结束的日子进台账，今天不进（今天由日环实时提供）。
func TestLedgerAbsorbsCompletedDays(t *testing.T) {
	c := New()
	now := time.Now()
	yesterday := now.AddDate(0, 0, -1)
	c.Record(sampleAt(yesterday, "u1", "m1", 10, 1.5))
	c.Record(sampleAt(now, "u1", "m1", 20, 2.5))

	s := c.Snapshot()
	lt := s.LongTerm
	if lt.CoverageDays < 1 {
		t.Fatalf("CoverageDays=%d，期望至少覆盖昨天", lt.CoverageDays)
	}
	// 昨天的行必须在台账里出现，且 credit 等于当时记录值。
	yIdx := c.days.idxOf(yesterday)
	row := c.ledgerIdx[yIdx]
	if row == nil {
		t.Fatalf("昨天(index=%d) 未被沉淀进台账", yIdx)
	}
	if row.Credit != 1.5 || row.Requests != 1 {
		t.Errorf("台账昨天行 = %+v，期望 1 请求 / 1.5 credit", row)
	}
	// 今天不沉淀：台账里不应有今天的行。
	if c.ledgerIdx[c.days.idxOf(now)] != nil {
		t.Error("今天的行不该进台账（应由日环实时提供）")
	}
	// 但今天的量必须体现在长期序列的末日上。
	last := len(lt.Credit) - 1
	if lt.Credit[last] != 2.5 {
		t.Errorf("序列末日 credit=%v，期望 2.5", lt.Credit[last])
	}
}

// TestLongTermSeriesWindowAndMerge 窗口裁剪、连续日期、缺日补零、维度下钻。
func TestLongTermSeriesWindowAndMerge(t *testing.T) {
	c := New()
	now := time.Now()
	// 三天前与昨天各记一笔，中间那天留空，验证缺日补零与连续性。
	c.Record(sampleAt(now.AddDate(0, 0, -3), "u1", "m1", 10, 1))
	c.Record(sampleAt(now.AddDate(0, 0, -1), "u2", "m2", 30, 3))

	lt := c.SnapshotDays(5).LongTerm
	if len(lt.Dates) != 5 || len(lt.Credit) != 5 || len(lt.Requests) != 5 {
		t.Fatalf("窗口长度 dates=%d credit=%d requests=%d，期望都是 5", len(lt.Dates), len(lt.Credit), len(lt.Requests))
	}
	if lt.CoverageDays != 2 {
		t.Errorf("CoverageDays=%d，期望 2（三天前 + 昨天）", lt.CoverageDays)
	}
	if lt.Since != lt.Dates[1] {
		t.Errorf("Since=%q，期望窗口内第一个有数据的日子 %q", lt.Since, lt.Dates[1])
	}
	if lt.Credit[1] != 1 || lt.Credit[3] != 3 {
		t.Errorf("credit 序列 = %v，期望第 2 位 1、第 4 位 3", lt.Credit)
	}
	if lt.Credit[2] != 0 {
		t.Errorf("空日 credit=%v，期望补零", lt.Credit[2])
	}
	// 维度下钻：两个账号各占自己那天，另一天为 0。
	if lt.Accounts["u1"].Credit[1] != 1 || lt.Accounts["u2"].Credit[3] != 3 {
		t.Errorf("账号维度错位: u1=%v u2=%v", lt.Accounts["u1"].Credit, lt.Accounts["u2"].Credit)
	}
	if lt.Models["m2"].Requests[3] != 1 {
		t.Errorf("模型维度 m2 第 4 位请求数=%d，期望 1", lt.Models["m2"].Requests[3])
	}
	// 窗口外（4 天前）不在序列里：请求 5 天窗口不覆盖三天前以外的数据。
	if ltWith2 := c.SnapshotDays(2).LongTerm; ltWith2.CoverageDays != 1 {
		t.Errorf("2 天窗口 CoverageDays=%d，期望 1（只剩昨天）", ltWith2.CoverageDays)
	}
}

// TestLedgerPersistRoundTrip 台账跨重启回放：离线的日子不该因为进程重启而丢失。
func TestLedgerPersistRoundTrip(t *testing.T) {
	dir := t.TempDir()
	hot := filepath.Join(dir, "metrics.json")
	cold := filepath.Join(dir, "ledger.json")

	c := NewWithStateLedger(hot, cold)
	now := time.Now()
	c.Record(sampleAt(now.AddDate(0, 0, -2), "u9", "m9", 42, 4.25))
	c.Flush()

	raw, err := os.ReadFile(cold)
	if err != nil {
		t.Fatalf("台账文件未写出: %v", err)
	}
	var pl persistLedger
	if err := json.Unmarshal(raw, &pl); err != nil {
		t.Fatalf("台账文件不是合法 JSON: %v", err)
	}
	if len(pl.Days) != 1 || pl.Days[0].Credit != 4.25 {
		t.Fatalf("台账文件内容 = %+v，期望 1 行 / 4.25 credit", pl.Days)
	}

	// 新进程（模拟重启）只读文件：长期序列必须还原出那天的消耗。
	c2 := NewWithStateLedger(hot, cold)
	lt := c2.SnapshotDays(30).LongTerm
	if lt.CoverageDays != 1 {
		t.Fatalf("回放后 CoverageDays=%d，期望 1", lt.CoverageDays)
	}
	var sum float64
	for _, v := range lt.Credit {
		sum += v
	}
	if sum != 4.25 {
		t.Errorf("回放后 credit 合计=%v，期望 4.25", sum)
	}
	dim9, ok := lt.Accounts["u9"]
	if !ok {
		t.Fatalf("回放后账号维度未还原，accounts=%v", lt.Accounts)
	}
	if dim9.Credit[len(lt.Credit)-3] != 4.25 {
		t.Errorf("回放后 u9 的 credit 错位: %v", dim9.Credit)
	}
}

// TestLedgerCapacityDropsOldest 台账容量上限：超限丢最旧，且索引同步清理。
func TestLedgerCapacityDropsOldest(t *testing.T) {
	c := New()
	for i := int64(0); i < LedgerDays+10; i++ {
		c.insertLedgerLocked(&LedgerDay{Index: 100000 + i, Requests: 1})
	}
	if len(c.ledger) != LedgerDays {
		t.Fatalf("台账长度=%d，期望上限 %d", len(c.ledger), LedgerDays)
	}
	if len(c.ledgerIdx) != LedgerDays {
		t.Errorf("索引长度=%d，期望与台账一致 %d", len(c.ledgerIdx), LedgerDays)
	}
	if got := c.ledger[0].Index; got != 100000+10 {
		t.Errorf("最旧行 index=%d，期望 %d（前 10 条被淘汰）", got, 100000+10)
	}
	if c.ledgerIdx[100000] != nil {
		t.Error("被淘汰的行仍留在索引里（内存泄漏）")
	}
	// 顺序必须保持升序，否则回放与合并都会错位。
	for i := 1; i < len(c.ledger); i++ {
		if c.ledger[i-1].Index >= c.ledger[i].Index {
			t.Fatalf("台账未保持升序：第 %d 位 %d >= 第 %d 位 %d", i-1, c.ledger[i-1].Index, i, c.ledger[i].Index)
		}
	}
}

// TestBalanceSnapshot 余额快照：同日覆盖为收盘值，变化才置 dirty，跨日各留一份。
func TestBalanceSnapshot(t *testing.T) {
	c := New()
	balances := map[string]int64{"u1": 100, "u2": 200}
	c.SetBalanceSource(func() map[string]int64 {
		out := make(map[string]int64, len(balances))
		for k, v := range balances {
			out[k] = v
		}
		return out
	})
	c.PollBalances()
	c.dirty.Store(false) // 只看「余额没变时会不会反复刷盘」

	// 同日重复采集同一份余额：不该再产生待落盘变更。
	c.PollBalances()
	if c.dirty.Load() {
		t.Error("余额未变化却置了 dirty（会导致每天无谓刷盘）")
	}
	// 余额变化：更新为当日最新值。
	balances["u1"] = 90
	c.PollBalances()
	if !c.dirty.Load() {
		t.Error("余额变化未置 dirty")
	}
	if got := c.days.at(time.Now()).balances["u1"]; got != 90 {
		t.Errorf("当日余额=%d，期望 90", got)
	}

	lt := c.SnapshotDays(3).LongTerm
	bal := lt.Accounts["u1"].Balance
	if bal == nil {
		t.Fatal("长期序列缺少 u1 的余额数组")
	}
	last := bal[len(bal)-1]
	if last == nil || *last != 90 {
		t.Errorf("末日余额=%v，期望 90", last)
	}
	// 没有快照的日子必须是 null（指针 nil），不能拿 0 冒充。
	if bal[0] != nil {
		t.Errorf("无快照的日子余额=%v，期望 null", *bal[0])
	}
}

// TestLedgerFlushOnlyOnChange 台账无变更时不反复写盘（每天至多一次的设计前提）。
func TestLedgerFlushOnlyOnChange(t *testing.T) {
	dir := t.TempDir()
	cold := filepath.Join(dir, "ledger.json")
	c := NewWithStateLedger("", cold)
	c.Record(sampleAt(time.Now().AddDate(0, 0, -1), "u1", "m1", 1, 1))
	c.Flush()

	fi1, err := os.Stat(cold)
	if err != nil {
		t.Fatalf("台账未写出: %v", err)
	}
	// 无新写入时再次 Flush：文件不应被重写（mtime 不变）。
	time.Sleep(10 * time.Millisecond)
	c.Flush()
	fi2, err := os.Stat(cold)
	if err != nil {
		t.Fatalf("台账读取失败: %v", err)
	}
	if !fi1.ModTime().Equal(fi2.ModTime()) {
		t.Error("台账无变更却被重写")
	}
}

// TestLedgerRowReflectsLateSample 跨零点才结束的请求会让日环补写昨天，
// 台账必须跟着更新（覆盖式同步），否则日环滚出后长期数据会偏小。
func TestLedgerRowReflectsLateSample(t *testing.T) {
	c := New()
	now := time.Now()
	yesterday := now.AddDate(0, 0, -1)
	c.Record(sampleAt(yesterday, "u1", "m1", 1, 1))
	c.Snapshot() // 触发一次沉淀

	// 模拟迟到的第二个样本仍归属昨天。
	c.Record(sampleAt(yesterday, "u1", "m1", 1, 2))
	c.Snapshot()

	row := c.ledgerIdx[c.days.idxOf(yesterday)]
	if row == nil {
		t.Fatal("台账缺少昨天行")
	}
	if row.Credit != 3 || row.Requests != 2 {
		t.Errorf("台账昨天行 = %d 请求 / %v credit，期望 2 / 3（覆盖式同步）", row.Requests, row.Credit)
	}
}

// TestLedgerDateLabelsUseCST 日期标签必须按 UTC+8 输出：日环按 UTC+8 切日，
// 若标签用 UTC 格式化，整条长期序列与导出的 CSV 会集体早一天。
// 用例刻意不假设当前时刻——标签错位 bug 的表现随运行时钟漂移，写死日期会时灵时不灵。
func TestLedgerDateLabelsUseCST(t *testing.T) {
	c := New()
	cstNow := time.Now().In(cstZone)
	// 锚点：北京时"今天 00:30"与"昨天 23:30"（跨越 UTC+8 的日界，但没有跨 UTC 日界）。
	midnight := time.Date(cstNow.Year(), cstNow.Month(), cstNow.Day(), 0, 30, 0, 0, cstZone)
	today, yesterday := midnight, midnight.Add(-time.Hour)

	c.Record(sampleAt(today, "u1", "m1", 10, 1))
	c.Record(sampleAt(yesterday, "u1", "m1", 10, 1))
	lt := c.SnapshotDays(3).LongTerm

	todayIdx := c.days.idxOf(time.Now())
	posOf := func(at time.Time) string {
		off := todayIdx - c.days.idxOf(at) // 距今天的槽数
		return lt.Dates[int64(len(lt.Dates))-1-off]
	}
	if got, want := posOf(today), cstNow.Format("2006-01-02"); got != want {
		t.Errorf("北京时今天的标签=%q，期望 %q", got, want)
	}
	if got, want := posOf(yesterday), cstNow.AddDate(0, 0, -1).Format("2006-01-02"); got != want {
		t.Errorf("北京时昨天的标签=%q，期望 %q（UTC 格式化会得到再前一天）", got, want)
	}
}
