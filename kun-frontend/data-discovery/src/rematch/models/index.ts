import { user } from './user';
import { route } from './route';
import { dataDiscovery } from './dataDiscovery';
import { datasetDetail } from './datasetDetail';
import { dataSettings } from './dataSettings';
import { glossary } from './glossary';

export interface RootModel {
  user: typeof user;
  route: typeof route;
  dataDiscovery: typeof dataDiscovery;
  datasetDetail: typeof datasetDetail;
  dataSettings: typeof dataSettings;
  glossary: typeof glossary;
}

export const models: RootModel = {
  user,
  route,
  dataDiscovery,
  datasetDetail,
  dataSettings,
  glossary,
};

// some common types
export interface Pagination {
  pageSize: number;
  pageNumber: number;
  totalCount?: number;
}

export interface Sort {
  sortOrder?: string;
  sortColumn?: string;
}

export enum DbType {
  MySQL = 'MySQL',
  PostgreSQL = 'PostgreSQL',
  Arrango = 'Arrango',
  Hive = 'Hive',
  Redis = 'Redis',
  MongoDB = 'MongoDB',
  Elasticsearch = 'Elasticsearch',
  Amazon_S3 = 'Amazon S3',
  HDFS = 'HDFS',
  FTP = 'FTP',
}
