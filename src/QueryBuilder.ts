import * as Knex from 'knex';

export enum PostgresOnConflictType {
    Constraint,
    Fields,
    UpdateDo
}

export interface PostgresOnConflict {
    type: PostgresOnConflictType,
    data: any // Constraint => constraint name(string), Fields => names (Array),
}

export class KnexORMQueryBuilder<ModelInterface> {
    private _knex: Knex;
    private _tableName?: string;
    private _rawBuilder?: Knex.QueryBuilder;
    private _tableIdColumn: string = 'id';
    private _insertReturning?: string | keyof ModelInterface | (keyof ModelInterface)[];
    private _trx?: Knex.Transaction;

    public constructor(knex: Knex, tableName?: string) {
        this._knex = knex;
        this._tableName = tableName;
    }

    public tableName(tableName: string) {
        this._tableName = tableName;
        return this;
    }

    public tableIdColumn(idColumn: string) {
        this._tableIdColumn = idColumn;
        return this;
    }

    public returning(
        columnsNames: string | keyof ModelInterface | (keyof ModelInterface)[] | undefined
    ) { // TODO: you may like to add '*' as a type
        this._insertReturning = columnsNames;
        return this;
    }

    public transacting(trx: Knex.Transaction) {
        this._trx = trx;
        return this;
    }

    public knex(knex: Knex) {
        this._knex = knex;
        return this;
    }

    public query() {
        return this._knex<ModelInterface>(this._tableName);
    }

    public transaction(
        callback: (
            getTableTrx: () => Knex.QueryBuilder,
            trx: Knex.Transaction
        ) => any
    ) {
        return this._knex.transaction((trx) => {
            return callback(
                () => trx(this._tableName),
                trx
            );
        });
    }

    /**
     * Make sure to define the table name first
     *
     * @param {ModelInterface[]} data
     * @param {boolean} [autoBulk=true]
     * @param {number} [bulkSize=1000]
     * @param {boolean} [bulk=false]
     * @returns
     * @memberof KnexORMQueryBuilder
     */
    public insertOrBatch(
        data: ModelInterface[],
        autoBulk: boolean = true,
        bulkSize: number = 1000,
        bulk: boolean = false,
    ) {
        if (autoBulk) {
            bulk = data.length > bulkSize;
        }

        if (bulk) {
            const query = this._knex.batchInsert(this._tableName as string, data, bulkSize);

            if (this._insertReturning) {
                query.returning(this._insertReturning as string | string[]);
            }

            if (this._trx) {
                query.transacting(this._trx);
            }
            return query
        } else {
            const query = this._knex(this._tableName).insert(data);

            if (this._insertReturning) {
                query.returning(this._insertReturning as string | string[]);
            }
            return query;
        }
    }


    public updateOrBatch(data: Partial<ModelInterface>[]) {
        return new Promise(async (resolve, reject) => {
            try {
                resolve(
                    await this._knex.transaction(async (trx) => {
                        const queries = data.map(
                            (d: Partial<ModelInterface>) =>
                                this._knex(this._tableName)
                                    .where(
                                        this._tableIdColumn,
                                        (d as any)[this._tableIdColumn]
                                    ).update(d)
                        );

                        try {
                            trx.commit(await Promise.all(queries));
                        } catch (err) {
                            trx.rollback(err);
                        }
                    })
                );
            } catch (err) {
                reject(err);
            }
        });
    }

    public async postgresInsertOrBatchIgnoreOnConflict(
        data: ModelInterface[],
        onConflictStatement: PostgresOnConflict,
        autoBulk: boolean = true,
        bulkSize: number = 1000,
        bulk: boolean = false,
    ) {
        if (autoBulk) {
            bulk = data.length > bulkSize;
        }

        if (bulk) {
            let nextFirstEl = 0;
            let returningList: any[] = [];

            console.log('>>postgresInsertBulk>>>> Bulk ==========+>')

            while (nextFirstEl < data.length) {
                console.log('>>postgresInsertBulk>>>> iter ==========+>' + nextFirstEl)

                const query = this.postgresInsertIgnoreOnConflict(
                    data.slice(nextFirstEl, Math.min(nextFirstEl + bulkSize, data.length - 1)),
                    onConflictStatement
                );

                if (this._insertReturning) {
                    // TODO: handle returning in some way (with the raw query str too)
                    // query.returning(this._insertReturning as string | string[]);
                }

                if (this._trx) {
                    query.transacting(this._trx);
                }

                const returns = await query;

                if (this._insertReturning) {
                    returningList = returningList.concat(returns);
                }

                nextFirstEl += bulkSize;
            }

            console.log("all inserted");

            return this._insertReturning ? returningList : null;
        } else {
            const query = this.postgresInsertIgnoreOnConflict(
                data,
                onConflictStatement
            );

            if (this._insertReturning) {
                // TODO: handle returning
                // query.returning(this._insertReturning as string | string[]);
            }

            if (this._trx) {
                query.transacting(this._trx);
            }

            return query;
        }
    }

    public postgresInsertIgnoreOnConflict(
        data: ModelInterface[],
        onConflictStatement: PostgresOnConflict
    ) {
        const fieldsNames = Object.keys(data[0]);

        const {
            str: valuesStr,
            bindData: valuesBindData
        } = this._getManyDataRawFieldsValuesStr(data, fieldsNames);

        let rawQuery = `INSERT INTO "${this._tableName}" ${this._getRawFieldsStr(fieldsNames)} VALUES ${valuesStr} ON CONFLICT `;

        // ___________________ add constraint statement
        switch (onConflictStatement.type) {
            case PostgresOnConflictType.Fields:
                rawQuery += `ON CONSTRAINT ${onConflictStatement.data}`;
            case PostgresOnConflictType.Fields:
                rawQuery += `(${onConflictStatement.data.map((field: string) => `"${field}"`).join(', ')})`;
        }

        // ____________________ add do nothing
        rawQuery += 'DO NOTHING;'

        return this._knex.raw(rawQuery, valuesBindData);
    }



    private _getRawFieldsStr(fieldsNames: string[]) {
        let result = '(';
        for (const field of fieldsNames) {
            result += `"${field}", `;
        }
        result = result.substr(0, result.length - 2) + ')';
        return result;
    }

    private _getManyDataRawFieldsValuesStr(
        data: ModelInterface[],
        fieldsNames: string[]
    ) {
        let resultStr = '';
        let bindData: any[] = [];
        for (const d of data) {
            const {
                str,
                bindData: oneDataBindData
            } = this._getOneDataRawFieldsValuesStr(d, fieldsNames);
            resultStr += `${str}, `;
            bindData = bindData.concat(oneDataBindData);
        }

        resultStr = resultStr.substr(0, resultStr.length - 2);

        return {
            str: resultStr,
            bindData
        }
    }

    private _getOneDataRawFieldsValuesStr(data: ModelInterface, fieldsNames: string[]) {
        let resultStr = '(';
        const bindData = [];
        for (const field of fieldsNames) {
            if ((data as any)[field]) {
                resultStr += `?, `;
                bindData.push((data as any)[field]);
            } else {
                resultStr += 'DEFAULT, ';
            }
        }
        resultStr = resultStr.substr(0, resultStr.length - 2) + ')';

        return {
            str: resultStr,
            bindData
        };
    }

    // query() {
    //     this._rawBuilder = this.rawQuery();
    //     return this;
    // }

    // rawQuery() {
    //     return this._knex<ModelInterface>(this._tableName);
    // }

    // select(...columnsNames: (keyof ModelInterface)[]) {
    //     return this._q('select', ...columnsNames);
    // }

    // where(...columnsNames: (keyof ModelInterface)[]) {
    //     return this._q('select', ...columnsNames);
    // }

    // andWhere(...columnsNames: (keyof ModelInterface)[]) {
    //     return this._q('select', ...columnsNames);
    // }



    // private _q(prop: keyof Knex.QueryBuilder, ...args: any[]) {
    //     if (this._rawBuilder) {
    //         this._rawBuilder = (this._rawBuilder[prop] as Function)(...args);
    //     }
    //     return this;
    // }
}
