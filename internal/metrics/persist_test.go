// persist_test.go 统计落盘测试：往返恢复、life 归零、原子写无残留、clean 不建文件、
// 损坏文件容错、落盘失败不 panic、后台 flusher、并发。
package metrics

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// tempState 返回 <t.TempDir()>/metrics.json 路径（沿用 pool 的 t.TempDir() 惯例）。
func tempState(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "metrics.json")
}

func TestPersistRoundTripRestoresRings(t *testing.T) {
	fp := tempState(t)
	c := NewWithState(fp)
	now := time.Now()
	c.Record(sample(now, "u1", "glm-5.2", 200, 100, 50, 1.25, true))
	c.Record(sample(now, "u2", "glm-5.2", 429, -1, 0, 0, false))
	c.Flush()

	if _, err := os.Stat(fp); err != nil {
		t.Fatalf("落盘文件不存在: %v", err)
	}

	// 重开：时间环应恢复，life 应清零（进程累计语义不变）。
	c2 := NewWithState(fp)
	s := c2.Snapshot()

	if s.Hours[len(s.Hours)-1].Requests != 2 {
		t.Errorf("恢复后本小时 requests=%d, want 2", s.Hours[len(s.Hours)-1].Requests)
	}
	if s.Hours[len(s.Hours)-1].Credit != 1.25 {
		t.Errorf("恢复后本小时 credit=%v, want 1.25", s.Hours[len(s.Hours)-1].Credit)
	}
	var dayCredit float64
	var dayReq int64
	for _, d := range s.Days {
		dayCredit += d.Credit
		dayReq += d.Requests
	}
	if dayReq != 2 || dayCredit != 1.25 {
		t.Errorf("恢复后日环 req=%d credit=%v, want 2/1.25", dayReq, dayCredit)
	}
	// life 必须归零：面板「本进程请求」标签的语义依赖这一点。
	if s.Total.Requests != 0 || s.Total.Credit != 0 {
		t.Errorf("life 应归零, got requests=%d credit=%v", s.Total.Requests, s.Total.Credit)
	}
	if len(s.ByUID) != 0 {
		t.Errorf("life by_uid 应归零, got %v", s.ByUID)
	}
}

func TestPersistDayDimsRestored(t *testing.T) {
	fp := tempState(t)
	c := NewWithState(fp)
	c.Record(sample(time.Now(), "u1", "deepseek", 200, 10, 5, 0.5, true))
	c.Flush()

	c2 := NewWithState(fp)
	s := c2.Snapshot()
	// 找到非空日槽，校验维度明细（by_uid/by_model/by_status）已恢复。
	var found bool
	for _, d := range s.Days {
		if d.Requests == 0 {
			continue
		}
		found = true
		if d.ByUID["u1"].Requests != 1 {
			t.Errorf("日槽 by_uid[u1] 未恢复: %+v", d.ByUID)
		}
		if d.ByModel["deepseek"].Requests != 1 {
			t.Errorf("日槽 by_model 未恢复: %+v", d.ByModel)
		}
		if d.ByStatus["200"] != 1 {
			t.Errorf("日槽 by_status 未恢复: %+v", d.ByStatus)
		}
	}
	if !found {
		t.Fatal("未找到非空日槽")
	}
}

func TestFlushCleanWritesNothing(t *testing.T) {
	fp := tempState(t)
	c := NewWithState(fp)
	c.Flush() // 无 Record → dirty=false → 不应建文件
	if _, err := os.Stat(fp); !os.IsNotExist(err) {
		t.Fatalf("clean 状态 Flush 不应建文件, stat err=%v", err)
	}
}

func TestPersistNoTmpLeftover(t *testing.T) {
	fp := tempState(t)
	c := NewWithState(fp)
	c.Record(sample(time.Now(), "u1", "m", 200, 1, 1, 0, false))
	c.Flush()
	if _, err := os.Stat(fp + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("原子写残留 .tmp 文件: err=%v", err)
	}
}

func TestPersistCorruptFileIgnored(t *testing.T) {
	fp := tempState(t)
	if err := os.WriteFile(fp, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := NewWithState(fp) // 损坏文件应静默忽略，退化为空历史
	s := c.Snapshot()
	if s.Total.Requests != 0 {
		t.Errorf("损坏文件后 requests=%d, want 0", s.Total.Requests)
	}
	// 恢复后仍能正常记录与落盘。
	c.Record(sample(time.Now(), "u1", "m", 200, 1, 1, 0, false))
	c.Flush()
	if _, err := os.Stat(fp); err != nil {
		t.Fatalf("损坏文件后无法重新落盘: %v", err)
	}
}

func TestPersistWriteFailureDoesNotPanic(t *testing.T) {
	dir := t.TempDir()
	block := filepath.Join(dir, "block")
	if err := os.WriteFile(block, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	// 父路径是文件 → MkdirAll/WriteFile 必失败。
	c := NewWithState(filepath.Join(block, "metrics.json"))
	c.Record(sample(time.Now(), "u1", "m", 200, 1, 1, 0, false))
	c.Flush() // 不应 panic

	if c.persistFails == 0 {
		t.Error("落盘失败未记账（persistFails 仍为 0）")
	}
	// 失败后 dirty 必须保留，以便下次重试。
	if !c.dirty.Load() {
		t.Error("落盘失败后 dirty 应保留以便重试")
	}
}

func TestPersistRetryAfterFailureRecovers(t *testing.T) {
	dir := t.TempDir()
	block := filepath.Join(dir, "block")
	if err := os.WriteFile(block, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := NewWithState(filepath.Join(block, "metrics.json"))
	c.Record(sample(time.Now(), "u1", "m", 200, 1, 1, 0, false))
	c.Flush() // 失败

	// 改到可写路径后重试：dirty 仍在 → 应成功落盘并清零失败计数。
	good := filepath.Join(dir, "ok", "metrics.json")
	c.stateFp = good
	c.Flush()
	if _, err := os.Stat(good); err != nil {
		t.Fatalf("重试未落盘: %v", err)
	}
	if c.persistFails != 0 {
		t.Errorf("成功后 persistFails 应清零, got %d", c.persistFails)
	}
	if c.dirty.Load() {
		t.Error("成功后 dirty 应清零")
	}
}

func TestPersistAutoFlush(t *testing.T) {
	old := flushInterval
	flushInterval = 20 * time.Millisecond
	defer func() { flushInterval = old }()

	fp := tempState(t)
	c := NewWithState(fp)
	c.Record(sample(time.Now(), "u1", "m", 200, 1, 1, 0, false))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(fp); err == nil {
			return // 后台 flusher 已落盘
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("后台 flusher 2s 内未落盘")
}

func TestPersistFileFormat(t *testing.T) {
	fp := tempState(t)
	c := NewWithState(fp)
	c.Record(sample(time.Now(), "u1", "m", 200, 5, 3, 0.75, true))
	c.Flush()

	raw, err := os.ReadFile(fp)
	if err != nil {
		t.Fatal(err)
	}
	var pm persistMetrics
	if err := json.Unmarshal(raw, &pm); err != nil {
		t.Fatalf("落盘文件非合法 JSON: %v", err)
	}
	if pm.SavedAt.IsZero() {
		t.Error("saved_at 未写入")
	}
	if len(pm.Hours) == 0 || len(pm.Days) == 0 {
		t.Fatalf("hours/days 未写入: hours=%d days=%d", len(pm.Hours), len(pm.Days))
	}
	// life 不应出现在文件里（结构上就没有该字段）。
	if strings.Contains(string(raw), "life") {
		t.Errorf("落盘文件不应包含 life 字段")
	}
	// 文件权限 0600（敏感度低，但与 pool 的 state.json 口径一致）。
	info, err := os.Stat(fp)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("文件权限=%o, want 600", perm)
	}
}

func TestPersistConcurrentRecordFlush(t *testing.T) {
	fp := tempState(t)
	c := NewWithState(fp)
	done := make(chan struct{})
	go func() {
		for i := 0; i < 200; i++ {
			c.Flush()
		}
		close(done)
	}()
	for i := 0; i < 500; i++ {
		c.Record(sample(time.Now(), "u1", "m", 200, 1, 1, 0.1, true))
	}
	<-done
	c.Flush()

	c2 := NewWithState(fp)
	s := c2.Snapshot()
	// 恢复的日环总量应等于写入量（可能因并发时序少写最后一次，但不应为 0）。
	var total int64
	for _, d := range s.Days {
		total += d.Requests
	}
	if total == 0 {
		t.Fatal("并发 Record/Flush 后恢复为空")
	}
	if total > 500 {
		t.Fatalf("恢复量 %d 超过写入量 500", total)
	}
}
