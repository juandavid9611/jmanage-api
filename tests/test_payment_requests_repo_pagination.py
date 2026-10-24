from repositories.payment_requests_repo_ddb import PaymentRequestsRepo


class _TwoPageTable:
    def __init__(self):
        self.calls = []

    def query(self, **kwargs):
        self.calls.append(kwargs)
        if "ExclusiveStartKey" not in kwargs:
            return {"Items": [{"id": "a"}, {"id": "b"}], "LastEvaluatedKey": {"id": "b"}}
        return {"Items": [{"id": "c"}]}


def test_list_all_follows_last_evaluated_key():
    repo = PaymentRequestsRepo.__new__(PaymentRequestsRepo)
    repo._table = _TwoPageTable()
    repo._account_gsi = "account_id_index"

    items = list(repo.list_all("vittoriacd"))

    assert [i["id"] for i in items] == ["a", "b", "c"]
    assert len(repo._table.calls) == 2
    assert repo._table.calls[1]["ExclusiveStartKey"] == {"id": "b"}
