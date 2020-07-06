import {
  ExtractRematchDispatchersFromEffects,
  ModelConfig,
  ModelEffects,
  Models,
  RematchRootState,
} from '@rematch/core';

/**
 * Common utility types
 * @author Josh Ouyang
 */

export interface BackendRespData<T = any> {
  code: number;
  message: string;
  result?: T;
  // success: boolean;
}

export type ServiceRespPromise<T = any> = Promise<T | null>;

export interface PaginationReqBody {
  pageNum: number;
  pageSize: number;
}

export interface PaginationRespBody<T = any> {
  pageSize: number;
  pageNum: number;
  totalCount: number;
  records: T[];
}

export interface SingleColumnSorter<T extends Record<string, any>> {
  field?: keyof T;
  order?: 'ascend' | 'descend';
}

export interface SingleColumnSortReqBody<T> {
  sortField: SingleColumnSorter<T> | undefined;
}

/**
 * Converts all fields of T into nullable fields
 * Example usage:
 * interface A { a: number; b: boolean; c: string; d: number[] }
 * type B = Nullable<A>;
 * -- is equivalent to --
 * interface B {
 *   a: number | null;
 *   b: boolean | null;
 *   c: string | null;
 *   d: number[] | null
 * }
 */
export type Nullable<T extends Record<any, any>> = {
  [K in keyof T]: T[K] | null;
};

/**
 * Converts all fields but U of T into nullable fields
 * Example usage:
 * interface A { a: number; b: boolean; c: string, d: number[] }
 * type B = NullableExcept<A, 'a' | 'c'>;
 * -- is equivalent to --
 * interface B {
 *   a: number;
 *   b: boolean | null;
 *   c: string;
 *   d: number[] | null;
 * }
 * Please note that at this point, A extends B (A is a subset of B)
 */
export type NullableExcept<T extends Record<any, any>, U extends keyof T> =
  Pick<T, U> & Nullable<Omit<T, U>>;

export type NullableExceptId<T extends Record<any, any> & { id: any }> = NullableExcept<T, 'id'>;

/*
* Type fixes for rematch loading plugin
* */
export type InferLoadingEffectsFromFunction<EffectFunction extends ((dispatch: any) => ModelEffects<any>)> = {
  [k in keyof ReturnType<EffectFunction>]: boolean;
};

export type WithLoading<
  RootState extends RematchRootState<any>,
  RootModel extends { [key in keyof RootModel]: ModelConfig }
  > = RootState & {
  loading: {
    global: boolean;
    models: {
      [modelName in keyof RootState]: boolean;
    };
    effects: {
      [modelName in keyof RootModel]: RootModel[modelName]['effects'] extends (dispatch: any) => ModelEffects<any> ?
        InferLoadingEffectsFromFunction<RootModel[modelName]['effects']> : {
          [effectName in keyof RootModel[modelName]['effects']]: boolean;
        };
    };
  };
};

export interface LoadingState<M extends Models> {
  loading: {
    global: boolean,
    models: { [modelName in keyof M]: boolean },
    effects: {
      [modelName in keyof M]: {
        [effectName in keyof ExtractRematchDispatchersFromEffects<M[modelName]['effects']>]: boolean
      }
    },
  }
}