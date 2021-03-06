import React, { memo, useMemo, useCallback } from 'react';
import { Table, Popconfirm, Tag, Tooltip } from 'antd';
import { ColumnsType, TablePaginationConfig } from 'antd/lib/table';
import { DeleteOutlined, CheckCircleFilled, CloseCircleFilled, CheckOutlined } from '@ant-design/icons';
import uniqueId from 'lodash/uniqueId';
import { DataQualityItem } from '@/rematch/models/datasetDetail';
import { DataQualityType, DataQualityHistoryStatus, DataQualityHistory } from '@/rematch/models/dataQuality';
import useI18n from '@/hooks/useI18n';
import useRedux from '@/hooks/useRedux';
import { dateFormatter } from '@/utils/datetime-utils';
import TestCaseRuleTable from '@/pages/monitoring-dashboard/components/data-discovery-board/TestCaseRuleTable';

import styles from './DataQualityTable.less';

interface Props {
  data: DataQualityItem[];
  onDelete: (id: string) => void;
  onClick: (id: string) => void;
}

const tagColorMap = {
  [DataQualityType.Accuracy]: 'orange',
  [DataQualityType.Completeness]: 'green',
  [DataQualityType.Consistency]: 'blue',
  [DataQualityType.Timeliness]: 'red',
  [DataQualityType.Uniqueness]: 'purple',
};

const colorMap = {
  warning: '#ff6336',
  green: '#9ac646',
  stop: '#526079',
};

export default memo(function DataQualityTable({ data, onDelete, onClick }: Props) {
  const t = useI18n();

  const { selector, dispatch } = useRedux(state => state.datasetDetail);

  const handleChangePagination = useCallback(
    (pageNumber: number, pageSize?: number) => {
      dispatch.datasetDetail.updateDataQualityPagination({
        pageNumber,
        pageSize: pageSize || 25,
      });
    },
    [dispatch.datasetDetail],
  );
  const handleChangePageSize = useCallback(
    (_pageNumber: number, pageSize: number) => {
      dispatch.datasetDetail.updateDataQualityPagination({
        pageNumber: 1,
        pageSize: pageSize || 25,
      });
    },
    [dispatch.datasetDetail],
  );

  const pagination: TablePaginationConfig = useMemo(
    () => ({
      size: 'small',
      total: selector.dataQualityTablePagination.totalCount,
      showSizeChanger: true,
      showQuickJumper: true,
      onChange: handleChangePagination,
      onShowSizeChange: handleChangePageSize,
      pageSize: selector.dataQualityTablePagination.pageSize,
      pageSizeOptions: ['25', '50', '100', '200'],
    }),
    [
      handleChangePageSize,
      handleChangePagination,
      selector.dataQualityTablePagination.pageSize,
      selector.dataQualityTablePagination.totalCount,
    ],
  );

  const columns: ColumnsType<DataQualityItem> = useMemo(
    () => [
      {
        key: 'name',
        dataIndex: 'name',
        title: t('dataDetail.dataQualityTable.name'),
        className: styles.nameColumn,
        width: 280,
        render: (name: string, record) => (
          <span className={styles.pointerLabel} onClick={() => onClick(record.id)}>
            {name}
          </span>
        ),
      },
      {
        key: 'isPrimary',
        dataIndex: 'isPrimary',
        title: t('dataDetail.dataQualityTable.isPrimary'),
        width: 100,
        render: (isPrimary: boolean) => {
          if (isPrimary) {
            return <CheckOutlined />;
          }
          return null;
        },
      },
      {
        key: 'types',
        dataIndex: 'types',
        title: t('dataDetail.dataQuality.type'),
        render: (types: DataQualityType[] | null) => (
          <div>
            {types &&
              types.map(type => (
                <Tag key={type} color={tagColorMap[type]}>
                  {t(`dataDetail.dataQuality.type.${type}`)}
                </Tag>
              ))}
          </div>
        ),
      },
      {
        key: 'updateTime',
        dataIndex: 'updateTime',
        title: t('dataDetail.dataQualityTable.updateTime'),
        className: styles.nameColumn,
        width: 150,
        render: updateTime => dateFormatter(updateTime),
      },
      {
        key: 'createTime',
        dataIndex: 'createTime',
        title: t('dataDetail.dataQualityTable.createTime'),
        className: styles.nameColumn,
        width: 150,
        render: createTime => dateFormatter(createTime),
      },

      {
        key: 'updater',
        dataIndex: 'updater',
        title: t('dataDetail.dataQualityTable.updater'),
        className: styles.nameColumn,
        width: 100,
      },
      {
        key: 'historyList',
        dataIndex: 'historyList',
        title: t('dataDetail.dataQualityTable.historyList'),
        render: (historyList: DataQualityHistory[]) => {
          return (
            <div className={styles.historyList}>
              {historyList?.map(history => {
                if (history.status === DataQualityHistoryStatus.SUCCESS) {
                  return (
                    <CheckCircleFilled
                      key={uniqueId()}
                      className={styles.historyIcon}
                      style={{ color: colorMap.green }}
                    />
                  );
                }
                if (history.status === DataQualityHistoryStatus.FAILED) {
                  if (!history.errorReason) {
                    return (
                      <Tooltip
                        title={<TestCaseRuleTable data={history.ruleRecords} />}
                        placement="right"
                        color="#ffffff"
                        overlayClassName={styles.TestCaseRuleTableTooltip}
                      >
                        <CloseCircleFilled
                          key={uniqueId()}
                          className={styles.historyIcon}
                          style={{ color: colorMap.warning }}
                        />
                      </Tooltip>
                    );
                  }
                  return (
                    <Tooltip
                      title={history.errorReason}
                      placement="right"
                      overlayClassName={styles.FailedREasonTooltip}
                    >
                      <CloseCircleFilled
                        key={uniqueId()}
                        className={styles.historyIcon}
                        style={{ color: colorMap.warning }}
                      />
                    </Tooltip>
                  );
                }
                return null;
              })}
            </div>
          );
        },
      },
      {
        key: 'operator',
        dataIndex: 'id',
        width: 30,
        render: (id: string) => (
          <Popconfirm
            title={t('dataDetail.dataquality.delete.title')}
            onConfirm={() => onDelete(id)}
            okText={t('common.button.confirm')}
            cancelText={t('common.button.cancel')}
          >
            <DeleteOutlined />
          </Popconfirm>
        ),
      },
    ],
    [onClick, onDelete, t],
  );

  return (
    <Table
      rowKey="id"
      loading={selector.fetchDataQualityLoading}
      className={styles.dataQualityTable}
      columns={columns}
      dataSource={data}
      pagination={pagination}
      onHeaderRow={() => ({
        className: styles.header,
      })}
      size="small"
    />
  );
});
