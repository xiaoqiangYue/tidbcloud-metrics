from utils import logger
from capacity_planner.capacity_planner import CapacityPlanner


class MetricsAnalyzer:
    def __init__(self, request, resource_usage_metrics, capacity, instance_cnt, plan_traffic_x, plan_resource_redundancy_x, log_file_name, log_level):
        self.logger = logger.setup_logger(__name__, log_file_name, log_level)
        self.request = request
        self.resource_usage_metrics = resource_usage_metrics
        self.capacity = capacity
        self.instance_cnt = instance_cnt
        self.plan_traffic_x = plan_traffic_x
        self.plan_resource_redundancy_x = plan_resource_redundancy_x
        self.capacity_planner = CapacityPlanner(self.resource_usage_metrics, self.plan_traffic_x, self.plan_resource_redundancy_x, log_file_name, log_level)

    def analyze(self):
        if self.resource_usage_metrics is not None:
            data = {"component": "{}".format(self.request['component']),
                   "name": "{}".format(self.request['name'])}

            for key, value in self.resource_usage_metrics.items():
                data[key] = value

            data["capacity"] = self.capacity
            data["instance_cnt"] = self.instance_cnt

            plan = self.capacity_planner.get_resource_capacity_plan(self.instance_cnt, self.capacity)
            if plan is not None:
                for k, v in plan.items():
                    data[k] = v

            return data
