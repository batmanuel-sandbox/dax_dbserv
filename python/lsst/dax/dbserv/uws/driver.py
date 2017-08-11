
class AsyncDriverError(Exception):
    pass



class AsyncDriver:

    def submit(self, query, db_url, user_context):
        pass

    def list(self, user_context):
        pass

    def job(self, job_id):
        pass


class QservDriver(AsyncDriver):
    pass


class SlurmDriver(AsyncDriver):
    pass
