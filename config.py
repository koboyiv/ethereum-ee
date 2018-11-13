import os

env = os.getenv('ENV', 'development')


class DB:
    @staticmethod
    def localhost():
        return 'mysql+pymysql://siamsquared:password@192.168.99.209/coinradars'

    @staticmethod
    def radars(db_name='radars'):
        if env == 'development':
            return 'mysql+pymysql://siamsquared:password@db.carbonradars.io/{}'.format(db_name)
        elif env == 'production':
            return 'mysql+pymysql://siamsquared:password@internal-db.carbonradars.io/{}'.format(db_name)
