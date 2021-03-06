import React, { memo, useMemo } from 'react';
import { ColumnProps } from 'antd/es/table';
import { FailedTestCase } from '@/services/monitoring-dashboard';
import { Card, Table, Tooltip } from 'antd';
import { dayjs } from '@/utils/datetime-utils';
import useI18n from '@/hooks/useI18n';
import { TableOnChangeCallback } from '@/definitions/common-types';
import { Link } from 'umi';
import SafeUrlAssembler from 'safe-url-assembler';
import TestCaseRuleTable from './TestCaseRuleTable';

import styles from './FailedTestCasesTable.less';

interface OwnProps {
  data: FailedTestCase[];
  pageNum: number;
  pageSize: number;
  total: number;
  onChange?: TableOnChangeCallback<FailedTestCase>;
  loading?: boolean;
}

type Props = OwnProps;

export const FailedTestCasesTable: React.FC<Props> = memo(
  function FailedTestCasesTable(props) {
    const { data, pageNum, pageSize, total, onChange, loading } = props;

    const t = useI18n();

    const columns: ColumnProps<FailedTestCase>[] = useMemo(
      () => [
        {
          key: 'ordinal',
          title: '#',
          width: 60,
          render: (txt: any, record: FailedTestCase, index: number) => (
            <span>{(pageNum - 1) * pageSize + index + 1}</span>
          ),
        },
        {
          dataIndex: 'caseName',
          key: 'caseName',
          title: t(
            'monitoringDashboard.dataDiscovery.failedTestCasesTable.caseName',
          ),
          render: (txt: string, record: FailedTestCase) => {
            return (
              <Link
                to={SafeUrlAssembler()
                  .template('/data-discovery/dataset/:datasetId')
                  .param({
                    datasetId: record.datasetGid,
                  })
                  .query({
                    caseId: record.caseId,
                  })
                  .toString()}
              >
                {txt}
              </Link>
            );
          },
        },
        {
          dataIndex: 'datasetName',
          key: 'datasetName',
          title: t(
            'monitoringDashboard.dataDiscovery.failedTestCasesTable.datasetName',
          ),
          render: (txt: string, record: FailedTestCase) => {
            return (
              <Link
                to={SafeUrlAssembler()
                  .template('/data-discovery/dataset/:datasetId')
                  .param({
                    datasetId: record.datasetGid,
                  })
                  .toString()}
              >
                {txt}
              </Link>
            );
          },
        },

        {
          dataIndex: 'errorReason',
          key: 'errorReason',
          title: t(
            'monitoringDashboard.dataDiscovery.failedTestCasesTable.result',
          ),
          ellipsis: true,
          render: (errorReason, record: FailedTestCase) => {
            if (errorReason) {
              return (
                <Tooltip
                  title={errorReason}
                  placement="right"
                  overlayClassName={styles.FailedREasonTooltip}
                >
                  <div
                    style={{
                      overflow: 'hidden',
                      textOverflow: 'ellipsis',
                      whiteSpace: 'nowrap',
                      width: '100%',
                    }}
                  >
                    {errorReason}
                  </div>
                </Tooltip>
              );
            }
            return (
              <Tooltip
                title={<TestCaseRuleTable data={record.ruleRecords} />}
                placement="right"
                color="#ffffff"
                overlayClassName={styles.TestCaseRuleTableTooltip}
              >
                <div
                  style={{
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    whiteSpace: 'nowrap',
                    width: '100%',
                  }}
                >
                  {record.ruleRecords
                    .map(rule => rule.originalValue)
                    .join(', ')}
                </div>
              </Tooltip>
            );
          },
        },
        {
          dataIndex: 'updateTime',
          key: 'updateTime',
          align: 'right',
          sorter: true,
          title: t(
            'monitoringDashboard.dataDiscovery.failedTestCasesTable.lastUpdatedTime',
          ),
          render: (txt: number) => dayjs(txt).format('YYYY-MM-DD HH:mm'),
        },
        {
          dataIndex: 'continuousFailingCount',
          key: 'continuousFailingCount',
          align: 'right',
          sorter: true,
          title: t(
            'monitoringDashboard.dataDiscovery.failedTestCasesTable.continuousFailingCount',
          ),
        },
        {
          dataIndex: 'caseOwner',
          key: 'caseOwner',
          align: 'right',
          title: t(
            'monitoringDashboard.dataDiscovery.failedTestCasesTable.caseOwner',
          ),
        },
      ],
      [t, pageNum, pageSize],
    );

    return (
      <Card bodyStyle={{ padding: '8px' }}>
        <h3>
          {t('monitoringDashboard.dataDiscovery.failedTestCasesTable.title')}
          {!!total && <span style={{ marginLeft: 4 }}>({total})</span>}
        </h3>
        <Table<FailedTestCase>
          loading={loading}
          dataSource={data}
          size="small"
          columns={columns}
          onChange={onChange}
          rowKey={r =>
            `${r.status}-${r.caseOwner}-${r.errorReason}-${r.updateTime}`
          }
          pagination={{
            current: pageNum,
            pageSize,
            total,
            simple: true,
          }}
        />
      </Card>
    );
  },
);
