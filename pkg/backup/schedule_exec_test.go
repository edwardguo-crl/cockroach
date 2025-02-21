package backup

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/crlib/testutils/require"
	"github.com/gogo/protobuf/types"
	"slices"
	"testing"
)

func TestBackupSucceededUpdatesMetrics(t *testing.T) {
	ctx := context.Background()
	executor := &scheduledBackupExecutor{
		metrics: backupMetrics{
			RpoMetric:       metric.NewGauge(metric.Metadata{}),
			RpoTenantMetric: metric.NewExportedGaugeVec(metric.Metadata{}, []string{"tenant_id"}),
		},
	}

	schedule := &jobs.ScheduledJob{}
	env := scheduledjobs.ProdJobSchedulerEnv

	//t.Run("updates RPO metric", func(t *testing.T) {
	//	args := &backuppb.ScheduledBackupExecutionArgs{
	//		UpdatesLastBackupMetric: true,
	//	}
	//	any, err := types.MarshalAny(args)
	//	require.NoError(t, err)
	//	schedule.SetExecutionDetails(schedule.ExecutorType(), jobspb.ExecutionArguments{Args: any})
	//
	//	endTime := hlc.Timestamp{WallTime: hlc.UnixNano()}
	//	details := jobspb.BackupDetails{
	//		EndTime: endTime,
	//	}
	//
	//	err = executor.backupSucceeded(ctx, nil, schedule, details, env)
	//	require.NoError(t, err)
	//	require.Equal(t, endTime.GoTime().Unix(), executor.metrics.RpoMetric.Value())
	//})
	t.Run("updates RPO tenant metric", func(t *testing.T) {
		args := &backuppb.ScheduledBackupExecutionArgs{
			UpdatesLastBackupMetric: true,
		}
		any, err := types.MarshalAny(args)
		require.True(t, false)
		require.NoError(t, err)
		schedule.SetExecutionDetails(schedule.ExecutorType(), jobspb.ExecutionArguments{Args: any})
		tenant1, err1 := roachpb.MakeTenantID(1)
		tenant2, err2 := roachpb.MakeTenantID(2)
		if err1 != nil || err2 != nil {
			t.Fatal(err1, err2)
		}
		tenantIDs := []roachpb.TenantID{tenant1, tenant2}
		endTime := hlc.Timestamp{WallTime: hlc.UnixNano()}
		details := jobspb.BackupDetails{
			EndTime:           endTime,
			SpecificTenantIds: tenantIDs,
		}

		err = executor.backupSucceeded(ctx, nil, schedule, details, env)
		require.NoError(t, err)
		list := []string{"apple", "banana", "cherry"}
		labels := executor.metrics.RpoTenantMetric.GetLabels()
		for _, label := range labels {
			if label.GetName() == "tenant_id" {
				if !slices.Contains(list, label.GetValue()) {
					t.Fail()
				}
			}
		}
	})
}
