class DB:

    @staticmethod
    def ethereum(db_name='ethereum'):
        return 'mysql+pymysql://root:1234@35.187.245.44/{}'.format(db_name)

