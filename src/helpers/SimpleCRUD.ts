import { KnexModel } from '../Model';

export class SimpleCRUD<ModelInterface> {
    private _modelClass: typeof KnexModel;

    constructor(modelClass: typeof KnexModel) {
        this._modelClass = modelClass;
    }

    async insert(data: ModelInterface[], autoBulk: boolean = true, bulkSize: number = 1000, bulk: boolean = false) {
        try {
            // [to do]  if the performance get a hit. Use a queue and insert 5 or 10 (which mean every 5s to 10 second to 15second) as you wish. It can be configurable
            return await this._modelClass.queryBuilder<ModelInterface>().insertOrBatch(data, autoBulk, bulkSize, bulk);
        } catch (err) {
            console.error(err);
            return Promise.reject(err);
        }
    }

    async update(data: ModelInterface[]) {
        try {
            // [to do]  if the performance get a hit. Use a queue and insert 5 or 10 (which mean every 5s to 10 second to 15second) as you wish. It can be configurable
            return await this._modelClass.queryBuilder<ModelInterface>().updateOrBatch(data);
        } catch (err) {
            console.error(err);
            return Promise.reject(err);
        }
    }

    async delete(values: any[], columnName: keyof ModelInterface) {
        try {
            // [to do]  if the performance get a hit. Use a queue and insert 5 or 10 (which mean every 5s to 10 second to 15second) as you wish. It can be configurable
            return await this._modelClass.query<ModelInterface>().whereIn(columnName as string, values).delete();
        } catch (err) {
            console.error(err);
            return Promise.reject(err);
        }
    }
}