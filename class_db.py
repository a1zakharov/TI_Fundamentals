from peewee import *

db = SqliteDatabase('shares.db')


class BaseModel(Model):
    class Meta:
        database = db


class DataShares(BaseModel):
    class Meta:
        db_table = 'Shares'

    ticker = CharField(max_length=8)
    figi = IntegerField()
    asset_uid = IntegerField()


if __name__ == "__main__":
    db.create_tables([DataShares])
    db.close()
