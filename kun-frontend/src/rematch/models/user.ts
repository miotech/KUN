import { history } from 'umi';
import produce from 'immer';

import { loginService, whoamiService, logoutService } from '@/services/user';

import { RootDispatch } from '../store';

export interface UserState {
  isLogin: boolean;
  username: string;
  whoamiLoading: boolean;
}

export const user = {
  state: {
    isLogin: false,
    username: '',
    whoamiLoading: false,
  } as UserState,

  reducers: {
    updateLogin: produce((draftState: UserState, payload: boolean) => {
      draftState.isLogin = payload;
    }),
    updateUserInfo: produce((draftState: UserState, payload) => {
      draftState.username = payload.username;
    }),
    updateWhoamiLoading: produce((draftState: UserState, payload: boolean) => {
      draftState.whoamiLoading = payload;
    }),
  },

  effects: (dispatch: RootDispatch) => ({
    async fetchLogin(payload: { username: string; password: string }) {
      const resp = await loginService(payload);
      if (resp) {
        const whoamiResp = await whoamiService();
        if (whoamiResp) {
          dispatch.user.updateLogin(true);
          dispatch.user.updateUserInfo({ username: whoamiResp.username });
          history.push('/');
        }
      }
    },

    async fetchWhoami() {
      dispatch.user.updateWhoamiLoading(true);
      const resp = await whoamiService();
      dispatch.user.updateWhoamiLoading(false);
      if (resp) {
        dispatch.user.updateLogin(true);
        dispatch.user.updateUserInfo({ username: resp.username });
      }
    },

    async fetchLogout() {
      const resp = await logoutService();
      if (resp) {
        dispatch.user.updateLogin(false);
      }
    },
  }),
};
