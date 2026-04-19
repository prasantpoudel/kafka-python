class Rebalancer:
    def rebalance(self, consumers: list[str], num_partitions: int) -> dict[str, list[int]]:
        if not consumers:
            return {}

        assignment = {c: [] for c in consumers}
        for partition_id in range(num_partitions):
            consumer = consumers[partition_id % len(consumers)]
            assignment[consumer].append(partition_id)

        return assignment
