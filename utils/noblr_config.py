import os
import yaml


class NoblrConfig:
    """"
    Configuration file parser and convenience class
    """

    def __init__(self, config_file):
        self._yml = yaml.full_load(config_file)

    def notify_email(self):
        email = self._yml['conf']['notify']['email']
        if isinstance(email, list):
            recipient = ','.join(email)
        else:
            recipient = email
        return recipient

    def fs_xls_path(self):
        return self._yml['conf']['localfs']['xls_path']

    def fs_sql_path(self):
        return self._yml['conf']['localfs']['sql_path']

    def get_secret_name(self):
        return self._yml['conf']['secrets']['secret_name']

    def get_secret_region(self):
        return self._yml['conf']['secrets']['secret_region']

    def get_postgres_jdbcUrl(self):
        return self._yml['conf']['postgresJDBC']['jdbcUrl']

    def get_postgres_jdbcDatabase(self):
        return self._yml['conf']['postgresJDBC']['jdbcDatabase']

    def get_postgres_jdbcSchema(self):
        return self._yml['conf']['postgresJDBC']['jdbcSchema']

    def get_postgres_user(self):
        return self._yml['conf']['postgresJDBC']['postgres_user']

    def get_postgres_pwd(self):
        return self._yml['conf']['postgresJDBC']['postgres_pwd']

    def get_gw_access_key(self):
        return self._yml['conf']['gw_s3_con']['accessKey']

    def get_gw_secret_key(self):
        return self._yml['conf']['gw_s3_con']['secretKey']

    def get_gw_bucket_name(self):
        return self._yml['conf']['gw_s3_con']['bucketName']


if __name__ == '__main__':
    configFile = "/conf/config_dev.yml"
    config = open(configFile, "r")

    print(f"local xls file path : {config.fs_xls_path()}")