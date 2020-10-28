import { useRouteMatch } from 'umi';
import React, { useState, useEffect, useCallback } from 'react';
import Card from '@/components/Card/Card';
import useRedux from '@/hooks/useRedux';
import LineageBoard from '@/components/LineageDiagram/LineageBoard';
import { LineageDagreNodeData } from '@/components/LineageDiagram/LineageBoard/LineageBoard';
import BackButton from '@/components/BackButton/BackButton';
import SideDropCard from './components/SideDropCard/SideDropCard';

import styles from './index.less';
import { transformNodes } from './helpers/transformNodes';
import { transformEdges } from './helpers/transformEdges';
import { LineageDirection } from '@/services/lineage';

export default function Lineage() {
  const { selector, dispatch } = useRedux(state => state.lineage);

  const match = useRouteMatch<{ datasetId: string }>();
  const [isExpanded, setIsExpanded] = useState(false);
  const [currentType, setCurrentType] = useState<'dataset' | 'task'>('dataset');

  useEffect(() => {
    dispatch.lineage.fetchInitialLineageGraphInfo(match.params.datasetId);
  }, [dispatch.lineage, match.params.datasetId]);

  const nodes = transformNodes(
    selector.graph.vertices,
    selector.selectedNodeId,
  );
  const edges = transformEdges(
    selector.graph.edges,
    `${selector?.selectedEdgeInfo?.sourceNodeId}-${selector?.selectedEdgeInfo?.destNodeId}`,
  );

  const handleClickNode = useCallback(
    (node: LineageDagreNodeData) => {
      setIsExpanded(true);
      setCurrentType('dataset');
      dispatch.lineage.updateState({
        key: 'selectedNodeId',
        value: node.id,
      });
      dispatch.lineage.updateState({
        key: 'selectedEdgeInfo',
        value: null,
      });
    },
    [dispatch.lineage],
  );
  const handleClickEdge = useCallback(
    (edgeInfo: { srcNodeId: string; destNodeId: string }) => {
      setIsExpanded(true);
      setCurrentType('task');
      dispatch.lineage.updateState({
        key: 'selectedEdgeInfo',
        value: {
          sourceNodeId: edgeInfo.srcNodeId,
          destNodeId: edgeInfo.destNodeId,
          sourceNodeName: '',
          destNodeName: '',
        },
      });
      dispatch.lineage.updateState({
        key: 'selectedNodeId',
        value: null,
      });
    },
    [dispatch.lineage],
  );

  const handleClickBackground = useCallback(() => {
    setIsExpanded(false);
    dispatch.lineage.batchUpdateState({
      selectedNodeId: null,
      selectedEdgeInfo: null,
    });
  }, [dispatch.lineage]);

  const handleExpandUpstream = useCallback(
    (id: string) => {
      dispatch.lineage.fetchStreamLineageGraphInfo({
        id,
        direction: LineageDirection.UPSTREAM,
      });
    },
    [dispatch.lineage],
  );

  const handleExpandDownstream = useCallback(
    (id: string) => {
      dispatch.lineage.fetchStreamLineageGraphInfo({
        id,
        direction: LineageDirection.DOWNSTREAM,
      });
    },
    [dispatch.lineage],
  );

  return (
    <div className={styles.page}>
      <BackButton
        defaultUrl={`/data-discovery/dataset/${match.params.datasetId}`}
      />

      <Card className={styles.content}>
        <LineageBoard
          nodes={nodes}
          edges={edges}
          loading={selector.graphLoading}
          onClickNode={handleClickNode}
          onClickEdge={handleClickEdge}
          onClickBackground={handleClickBackground}
          onExpandUpstream={handleExpandUpstream}
          onExpandDownstream={handleExpandDownstream}
        />

        <SideDropCard
          isExpanded={isExpanded}
          datasetId={selector.selectedNodeId}
          sourceDatasetId={selector.selectedEdgeInfo?.sourceNodeId}
          destDatasetId={selector.selectedEdgeInfo?.destNodeId}
          sourceDatasetName={selector.selectedEdgeInfo?.sourceNodeName}
          destDatasetName={selector.selectedEdgeInfo?.destNodeName}
          onExpand={(v: boolean) => setIsExpanded(v)}
          type={currentType}
        />
      </Card>
    </div>
  );
}
