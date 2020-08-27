import { IConfig, defineConfig } from 'umi';
import fs from 'fs';
import path from 'path';
import CopyWebpackPlugin from 'copy-webpack-plugin';
import { appRoutes } from './routes';
import { theme } from './theme';
import { define } from './define';

let certConfigTemplate = {
  key: undefined,
  cert: undefined,
};
if (fs.existsSync(path.resolve(__dirname, './certConfig.json'))) {
  certConfigTemplate = {
    ...JSON.parse(fs.readFileSync(path.resolve(__dirname, './certConfig.json'), { encoding: 'utf-8' }))
  };
}

const {
  PROXY_TARGET,
  USE_MOCK,
  PROXY_PDF_TARGET,
  PATH_REWRITE,
  NO_PROXY,
  PORT,
  HTTPS,
  HTTPS_KEY,
  HTTPS_CERT,
} = process.env;

export default defineConfig({
  dynamicImport: {
    loading: '@/layouts/LoadingLayout/index',
  },
  hash: true,
  nodeModulesTransform: {
    type: 'none',
  },
  chainWebpack(memo) {
    memo.plugin('copy-cmaps').use(CopyWebpackPlugin, [
      {
        patterns: [
          {
            from: path.join(__dirname, '../', 'node_modules/pdfjs-dist/cmaps/'),
            to: 'cmaps/',
          },
        ],
      },
    ]);
  },
  targets: {
    ie: 11,
  },
  proxy: (!NO_PROXY) ? {
    '/kun/api/v1/': {
      target: PROXY_TARGET || 'http://kun-dev.miotech.com/',
      changeOrigin: true,
      pathRewrite: PATH_REWRITE ? { '^/kun/api/v1/' : '' } : undefined,
    },
    '/kun/api/data-platform/': {
      target: PROXY_TARGET || 'http://kun-dev.miotech.com/',
      changeOrigin: true,
      pathRewrite: PATH_REWRITE ? { '^/kun/api/data-platform/' : '' } : undefined,
    },
    '/kun/api/v1/pdf/': {
      target: PROXY_PDF_TARGET || 'http://kun-dev.miotech.com/',
      changeOrigin: true,
    },
  } : {},
  theme,
  lessLoader: {
    modifyVars: {
      hack: `true; @import "~@/styles/variables.less"; @import "~@/styles/mixins.less"`,
    },
  },
  locale: {
    default: 'zh-CN',
    antd: true,
    title: true,
    baseNavigator: true,
    baseSeparator: '-',
  },
  favicon: '/favicon.ico',
  title: 'common.app.name',
  routes: appRoutes,
  devServer: {
    host: HTTPS ? 'dev.localhost.com' : undefined,
    port: PORT ? parseInt(PORT, 10) : 8000,
    https: HTTPS
      ? {
          key: HTTPS_KEY || certConfigTemplate.key,
          cert: HTTPS_CERT || certConfigTemplate.cert,
        }
      : undefined,
  },
  define,
  mock: (USE_MOCK === 'true') ? {} : false,
}) as IConfig;
