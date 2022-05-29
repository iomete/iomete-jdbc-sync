class SyncMode:
    pass


class FullLoad(SyncMode):
    def __str__(self):
        return "full_load"


class IncrementalSnapshot(SyncMode):
    def __init__(self, identification_column: str = "id", tracking_column: str = "updated_at"):
        self.identification_column = identification_column
        self.tracking_column = tracking_column

    def __str__(self):
        return f"incremental_snapshot(identification_column: '{self.identification_column}', tracking_column: '{self.tracking_column}')"
