class ReindexNamespaceYetError(RuntimeError):
    def __init__(self):
        super().__init__('Ещё производится переиндексация')


class NotExistentReindexTaskError(RuntimeError):
    def __init__(self):
        super().__init__('Не создавалась задача на переиндексацию')
