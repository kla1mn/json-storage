class ReindexNamespaceYetError(RuntimeError):
    def __init__(self):
        super().__init__('Ещё производится переиндексация')
