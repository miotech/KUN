import React, { memo, useCallback, useEffect, useState } from 'react';
import {
  ReflexContainer,
  ReflexSplitter,
  ReflexElement,
} from 'react-reflex';
import { useMount, useUnmount } from 'ahooks';
import useRedux from '@/hooks/useRedux';
import useDebouncedUpdateEffect from '@/hooks/useDebouncedUpdateEffect';

import { TaskDefinitionFilterToolbar } from '@/pages/data-development/components/FilterToolbar/TaskDefinitionFilterToolbar';
import { TaskViewsAside } from '@/pages/data-development/components/TaskViewsAside/TaskViewsAside';
import { TaskDefinitionTable } from '@/pages/data-development/components/TaskDefinitionTable/TaskDefinitionTable';
import { TaskDefViewModificationModal } from '@/pages/data-development/components/TaskDefViewModificationModal/TaskDefViewModificationModal';
import { TaskTemplateCreateDropMenu } from '@/pages/data-development/components/TaskTemplateCreateDropMenu/TaskTemplateCreateDropMenu';

import {
  createTaskDefinitionView,
  deleteTaskDefinitionView,
  putTaskDefinitionsIntoView,
  updateTaskDefinitionView,
} from '@/services/data-development/task-definition-views';
import { createTaskDefinition } from '@/services/data-development/task-definitions';

import {
  TaskDefinitionViewBase, TaskDefinitionViewUpdateVO, TaskDefinitionViewVO
} from '@/definitions/TaskDefinitionView.type';
import { DataDevelopmentModelFilter } from '@/rematch/models/dataDevelopment/model-state';

import 'react-reflex/styles.css';
import { TaskDefToViewTransferModal } from '@/pages/data-development/components/TaskDefToViewTransfererModal/TaskDefToViewTransferModal';
import styles from './index.less';


const DataDevelopmentPage: React.FC<any> = memo(function DataDevelopmentPage() {
  const {
    selector: {
      filters,
      displayType,
      taskDefViewsList,
      loadingViews,
      selectedView,
    },
    dispatch,
  } = useRedux<{
    filters: DataDevelopmentModelFilter,
    displayType: 'LIST' | 'DAG',
    taskDefViewsList: TaskDefinitionViewBase[],
    loadingViews: boolean,
    selectedView: TaskDefinitionViewBase | null,
    loadingTaskDefs: boolean,
  }>(s => ({
    filters: s.dataDevelopment.filters,
    displayType: s.dataDevelopment.displayType,
    taskDefViewsList: s.dataDevelopment.taskDefViewsList,
    loadingViews: s.loading.effects.dataDevelopment.fetchTaskDefViews,
    selectedView: s.dataDevelopment.selectedTaskDefView,
    loadingTaskDefs: s.loading.effects.dataDevelopment.fetchTaskDefinitions,
  }));

  const [ taskDefViewSearchKeyword, setTaskDefViewSearchKeyword ] = useState<string>('');
  const [ createViewModalVisible, setCreateViewModalVisible ] = useState<boolean>(false);
  const [ editView, setEditView ] = useState<TaskDefinitionViewVO | null>(null);
  const [ transferModalVisible, setTransferModalVisible ] = useState<boolean>(false);
  const [ updateTime, setUpdateTime ] = useState<number>(Date.now());

  useMount(() => {
  });

  useUnmount(() => {
    // reset state & free up memory
    dispatch.dataDevelopment.setCreatingTaskTemplate(null);
  });

  /* Task definition view effects and callbacks */

  const searchTaskDefViews = () => {
    dispatch.dataDevelopment.fetchTaskDefViews({
      keyword: taskDefViewSearchKeyword,
    });
  };

  useEffect(() => {
    searchTaskDefViews();
    // eslint-disable-next-line
  }, []);

  useDebouncedUpdateEffect(() => {
    searchTaskDefViews();
  }, [
    taskDefViewSearchKeyword,
  ], {
    wait: 1000,
  });

  const handleCreateView = useCallback(async (updateVO: TaskDefinitionViewUpdateVO) => {
    try {
      await createTaskDefinitionView({
        name: updateVO.name,
        taskDefinitionIds: updateVO.taskDefinitionIds,
      });
      setCreateViewModalVisible(false);
    } finally {
      searchTaskDefViews();
    }
  }, [
    searchTaskDefViews,
  ]);

  const handleUpdateView = useCallback(async (updateVO: TaskDefinitionViewUpdateVO) => {
    if (editView) {
      try {
        await updateTaskDefinitionView(editView.id, {
          name: updateVO.name,
          taskDefinitionIds: editView.includedTaskDefinitionIds || [],
        });
        setEditView(null);
      } finally {
        searchTaskDefViews();
      }
    }
  }, [
    editView,
    searchTaskDefViews,
  ]);

  const handleDeleteView = useCallback(async (viewId: string) => {
    try {
      await deleteTaskDefinitionView(viewId);
      setEditView(null);
    } finally {
      searchTaskDefViews();
    }
  }, []);

  /* Task definition table renderer */
  const renderGraphOrTable = () => {
    if (displayType === 'LIST') {
      return (
        <div key="list-table" className={styles.tableComponentWrapper}>
          <TaskDefinitionTable
            taskDefViewId={selectedView?.id || null}
            filters={filters}
            updateTime={updateTime}
            onTransferToThisViewClicked={() => {
              setTransferModalVisible(true);
            }}
          />
        </div>
      );
    }
      return null;
  };

  const handleTaskDefinitionCreate = useCallback(async (taskTemplateName: string, name: string, createInCurrentView: boolean) => {
    try {
      const createdTaskDefinition = await createTaskDefinition({
        name,
        taskTemplateName,
      });
      if (createInCurrentView && selectedView && createdTaskDefinition) {
        await putTaskDefinitionsIntoView(selectedView.id, [createdTaskDefinition.id as string]);
      }
    } finally {
      searchTaskDefViews();
      setUpdateTime(Date.now);
    }
  }, [
    selectedView,
  ]);

  return (
    <main className={styles.Page}>
      <TaskTemplateCreateDropMenu
        onCreateTaskDefinition={handleTaskDefinitionCreate}
      />
      {/* Layout */}
      <ReflexContainer
        orientation="vertical"
      >
        {/* Task types select left aside */}
        <ReflexElement
          className={styles.leftPane}
          flex={0.192}
          minSize={200}
        >
          <TaskViewsAside
            loading={loadingViews}
            views={taskDefViewsList as any[]}
            onSearch={(searchText: string) => {
              setTaskDefViewSearchKeyword(searchText);
            }}
            onClickCreateBtn={() => {
              setCreateViewModalVisible(true);
            }}
            onSelectItem={(viewItem) => {
              dispatch.dataDevelopment.setSelectedTaskDefinitionView(viewItem);
            }}
            onEdit={(view) => {
              setEditView(view);
            }}
            selectedView={selectedView}
          />
        </ReflexElement>

        {/* Splitter */}
        <ReflexSplitter propagate />

        {/* Center task DAG graph / list table container */}
        <ReflexElement
          className={styles.mainPane}
          flex={0.847}
          minSize={800}
        >
          <TaskDefinitionFilterToolbar />
          {renderGraphOrTable()}
        </ReflexElement>
      </ReflexContainer>

      {/* Floating elements */}
      <TaskDefViewModificationModal
        mode="create"
        visible={createViewModalVisible}
        onOk={handleCreateView}
        onCancel={() => {
          setCreateViewModalVisible(false);
        }}
      />
      <TaskDefViewModificationModal
        mode="edit"
        visible={editView != null}
        initView={editView}
        onOk={handleUpdateView}
        onDelete={handleDeleteView}
        onCancel={() => {
          setEditView(null);
        }}
      />
      {/* Transfer tasks to current selected view */}
      <TaskDefToViewTransferModal
        viewsList={taskDefViewsList}
        onCancel={() => {
          setTransferModalVisible(false);
        }}
        visible={transferModalVisible}
        lockTargetView
        initTargetView={transferModalVisible ? selectedView : null}
      />
    </main>
  );
});

export default DataDevelopmentPage;
