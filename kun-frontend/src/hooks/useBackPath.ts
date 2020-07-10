import { useHistory } from 'umi';
import { useCallback } from 'react';
import qs from 'qs';

// 得到带有backurl的新url
export default function useBackPath() {
  const history = useHistory();

  const { location } = history;

  const { pathname } = location;

  const getBackPath = useCallback(
    (url: string) => {
      let query: any = {};
      const paramStr = url.split('?')[1];
      const urlNoSearchPath = url.split('?')[0];
      if (paramStr) {
        query = qs.parse(paramStr);
      }
      query.backUrl = pathname;
      return `${urlNoSearchPath}?${qs.stringify(query)}`;
    },
    [pathname],
  );

  return { getBackPath, pathname };
}
