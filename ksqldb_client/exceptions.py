class APIError(Exception):

    """All API endpoints use a standard error message format for any requests that return an HTTP status indicating an error (4xx/5xx)."""

    def __init__(self, json_error: dict):
        self.error_code = json_error["error_code"]
        self.message = json_error["message"]
        super().__init__(self.message)

    def __str__(self):
        """Return a meaningful string representation of the error."""
        return f"APIError: {self.error_code} - {self.message}"
