package builtins

import (
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeofday"
)

func TestTimeBuiltinIntOverload(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Find the time builtin with INT overload
	timeBuiltin, exists := builtins["time"]
	if !exists {
		t.Fatal("time builtin not found")
	}

	var intOverload tree.Overload
	for _, ov := range timeBuiltin.overloads {
		if ov.Types.MatchLen(1) && ov.Types.GetAt(0).Equivalent(types.Int) {
			intOverload = ov
			break
		}
	}

	if intOverload.Fn == nil {
		t.Fatal("time(INT) overload not found")
	}

	testCases := []struct {
		milliseconds int64
		expectedHour int
		expectedMin  int
		expectedSec  int
		description  string
	}{
		{1000, 0, 0, 1, "1000ms -> 00:00:01"},
		{3600000, 1, 0, 0, "3600000ms -> 01:00:00"},
		{3661000, 1, 1, 1, "3661000ms -> 01:01:01"},
		{0, 0, 0, 0, "0ms -> 00:00:00"},
		{1234567, 0, 20, 34, "1234567ms -> 00:20:34"},
		{86400000, 0, 0, 0, "86400000ms -> 00:00:00 (24 hours)"},
		{3723000, 1, 2, 3, "3723000ms -> 01:02:03"},
	}

	evalCtx := tree.NewTestingEvalContext(nil)

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			args := tree.Datums{tree.NewDInt(tree.DInt(tc.milliseconds))}

			result, err := intOverload.Fn(evalCtx, args)
			if err != nil {
				t.Fatalf("time(%d) failed: %v", tc.milliseconds, err)
			}

			if result.ResolvedType().Family() != types.TimeFamily {
				t.Fatalf("expected TIME result, got %s", result.ResolvedType())
			}

			timeResult := result.(*tree.DTime)
			timeOfDay := timeofday.TimeOfDay(*timeResult)
			hour, min, sec := timeOfDay.Hour(), timeOfDay.Minute(), timeOfDay.Second()

			if hour != tc.expectedHour {
				t.Errorf("hour mismatch: expected %d, got %d", tc.expectedHour, hour)
			}
			if min != tc.expectedMin {
				t.Errorf("minute mismatch: expected %d, got %d", tc.expectedMin, min)
			}
			if sec != tc.expectedSec {
				t.Errorf("second mismatch: expected %d, got %d", tc.expectedSec, sec)
			}

			t.Logf("✓ time(%d) = %02d:%02d:%02d", tc.milliseconds, hour, min, sec)
		})
	}
}

func TestTimeToSecBuiltins(t *testing.T) {
	defer leaktest.AfterTest(t)()

	timeToSecBuiltin, exists := builtins["time_to_sec"]
	if !exists {
		t.Fatal("time_to_sec builtin not found")
	}

	evalCtx := tree.NewTestingEvalContext(nil)

	t.Run("TIME_to_INT", func(t *testing.T) {
		var timeOverload tree.Overload
		for _, ov := range timeToSecBuiltin.overloads {
			if ov.Types.MatchLen(1) && ov.Types.GetAt(0).Equivalent(types.Time) {
				timeOverload = ov
				break
			}
		}

		if timeOverload.Fn == nil {
			t.Fatal("time_to_sec(TIME) overload not found")
		}

		timeInput := tree.MakeDTime(timeofday.New(1, 2, 3, 0))
		args := tree.Datums{timeInput}

		result, err := timeOverload.Fn(evalCtx, args)
		if err != nil {
			t.Fatalf("time_to_sec(%s) failed: %v", timeInput, err)
		}

		if result.ResolvedType().Family() != types.IntFamily {
			t.Fatalf("expected INT result, got %s", result.ResolvedType())
		}

		expectedSeconds := int64(1*3600 + 2*60 + 3)
		actualSeconds := int64(tree.MustBeDInt(result))

		if actualSeconds != expectedSeconds {
			t.Errorf("expected %d seconds, got %d", expectedSeconds, actualSeconds)
		}

		t.Logf("✓ time_to_sec(%s) = %d seconds", timeInput, actualSeconds)
	})

	t.Run("TIMESTAMP_to_INT", func(t *testing.T) {
		var timestampOverload tree.Overload
		for _, ov := range timeToSecBuiltin.overloads {
			if ov.Types.MatchLen(1) && ov.Types.GetAt(0).Equivalent(types.Timestamp) {
				timestampOverload = ov
				break
			}
		}

		if timestampOverload.Fn == nil {
			t.Fatal("time_to_sec(TIMESTAMP) overload not found")
		}

		timestampInput := tree.MakeDTimestamp(time.Date(2023, 1, 1, 1, 2, 3, 0, time.UTC), time.Nanosecond)
		args := tree.Datums{timestampInput}

		result, err := timestampOverload.Fn(evalCtx, args)
		if err != nil {
			t.Fatalf("time_to_sec(%s) failed: %v", timestampInput, err)
		}

		if result.ResolvedType().Family() != types.IntFamily {
			t.Fatalf("expected INT result, got %s", result.ResolvedType())
		}

		expectedSeconds := int64(1*3600 + 2*60 + 3)
		actualSeconds := int64(tree.MustBeDInt(result))

		if actualSeconds != expectedSeconds {
			t.Errorf("expected %d seconds, got %d", expectedSeconds, actualSeconds)
		}

		t.Logf("✓ time_to_sec(%s) = %d seconds", timestampInput, actualSeconds)
	})
}
