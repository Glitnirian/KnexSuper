import * as Knex from 'knex';
import { KnexORMQueryBuilder } from './QueryBuilder';
import { string } from 'prop-types';


export abstract class KnexModel<ModelDataInterface> {
    static knex: Knex;
    static idName: string = 'id';

    static getDefaultTableName: () => string;

    static tableOrDefault(tableName?: string) {
        if (tableName) {
            return tableName;
        }
        return this.getDefaultTableName();
    }

    static queryBuilder<ModelDataInterface>(tableName?: string, knex?: Knex) {
        if (!knex) {
            knex = this.knex;
        }

        if (!tableName) {
            tableName = this.getDefaultTableName();
        }

        return new KnexORMQueryBuilder<ModelDataInterface>(knex, tableName);
    }

    static query<ModelDataInterface>(tableName?: string, knex?: Knex) {
        return this.queryBuilder<ModelDataInterface>(tableName, knex).query();
    }

    static findById(id: any, tableName?: string, knex?: Knex) {
        return this.query(tableName, knex).where({[this.idName]: id}).first();
    }

    /**
     *
     *
     * @static
     * @template ModelDataInterface
     * @param {(
     *             getTrxQb: () => Knex.QueryBuilder,  // return trx(this.tableName)
     *             trx: Knex.Transaction
     *         ) => void} callback
     * @param {string} [tableName]
     * @param {Knex} [knex]
     * @returns
     * @memberof KnexModel
     * 
     * @example
     * MyExtendingModel.transaction(async (trx) => {
     *      await trx().update( // <- 
     *          {
     *             running: true
     *          }
     *      )
     *      .where('running': false);
     * 
     *      return trx().update({
     *          run: true
     *      })
     *      .whereIn('name', ['name1', 'name2', 'name3']);
     * });
     * 
     */
    static transaction<ModelDataInterface>(
        callback: (
            getTrxQb: () => Knex.QueryBuilder, 
            trx: Knex.Transaction
        ) => any,
        tableName?: string,
        knex?: Knex
    ) {
        return this.queryBuilder<ModelDataInterface>(tableName, knex).transaction(callback);
    }


    static modelize<ModelDataInterface, Model>(models: ModelDataInterface[] | ModelDataInterface, tableName?: string): Model | Model[] {
        if (Array.isArray(models)) {
            return models.map(
                model => new (this as any)(model, this.tableOrDefault(tableName))
            );
        } else {
            return new (this as any) (models, this.tableOrDefault(tableName));
        }
    }

    protected _data?: ModelDataInterface;
    protected _tableName: string;

    constructor(data: ModelDataInterface, tableName?: string) {
        this._data = data;
        Object.assign(this, data);
        this._tableName = tableName? tableName : KnexModel.getDefaultTableName();
    }

    get data() {
        return this._data;
    }
}