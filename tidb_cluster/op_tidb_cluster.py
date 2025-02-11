from prometheus.prom_class_promotion import PromotionPrometheusConnect
from datetime import datetime

class OpTidbCluster:
    def __init__(self, conf):
        """
        初始化 prometheus client 和相关参数
        """
        self.conf = conf
        self.conf["start_time"]=datetime.strptime(self.conf["start_time"], "%d/%m/%Y %H:%M:%S")
        self.conf["end_time"]=datetime.strptime(self.conf["end_time"], "%d/%m/%Y %H:%M:%S")
        # self.prome_url=self.get_prome_url_from_clinic_api()
        self.prome_client = self.get_prome_client()
        # self.queries = self.query_list()
    def get_all_tidb_instance(self):
        """
        获取所有 TiDB hostname
        :return: list
        """
        data=self.prome_client.custom_query_range_promotion(query="tidb_server_connections",start_time=self.conf["start_time"],end_time=self.conf["end_time"],step=self.conf["step_in_seconds"])
        hostnames = [entry['metric']['hostname'] for entry in data]
        return hostnames
    def get_all_pd_instance(self):
        """
        获取所有 PD hostname
        :return: list
        """
        data=self.prome_client.custom_query_range_promotion(query="pd_service_maxprocs",start_time=self.conf["start_time"],end_time=self.conf["end_time"],step=self.conf["step_in_seconds"])
        hostnames = [entry['metric']['hostname'] for entry in data]
        return hostnames       

    def get_all_tiflash_instance(self):
        """
        获取所有 PD hostname
        :return: list
        """
        data=self.prome_client.custom_query_range_promotion(query="pd_service_maxprocs",start_time=self.conf["start_time"],end_time=self.conf["end_time"],step=self.conf["step_in_seconds"])
        hostnames = [entry['metric']['hostname'] for entry in data]
        return hostnames 

    # def get_all_tikv_instance(self):

    # def get_all_ticdc_instance(self):

    def get_prome_client(self):
        """
        获取 Prometheus API 客户端对象
        :return: PrometheusClient 实例
        """
        if self.conf["headers"]["Authorization"]:
            prome_client = PromotionPrometheusConnect(url=self.conf["metrics_api_url"], disable_ssl=False,
                                   headers=self.conf["headers"])
        else:
            prome_client = PromotionPrometheusConnect(url=self.conf["metrics_api_url"], disable_ssl=False,
                                       headers=None)
        return prome_client
    
    
    # def get_prome_url_from_clinic_api(self):
    #     """
    #     通过 Clinic API 获取 prome_url 和 token
    #     :return: Prometheus url 地址
    #     """
    #     return self.prome_url

    # def get_custom_metric(self, query, start=None, end=None, step=60):
    #     """
    #     获取自定义的监控数据
    #     :param query: Prometheus 查询语句
    #     :param start: 查询的开始时间（Unix 时间戳）
    #     :param end: 查询的结束时间（Unix 时间戳）
    #     :param step: 查询的时间步长
    #     :return: 查询结果的 DataFrame
    #     """
    #     return self.prome_client.custom_query_range_promotion(query, start, end, step)

    def query_list(self,cluster_id):
        """
        定义查询语句，获取 TiDB 监控指标数据
        :return: 查询语句字典
        """
        
        queries["physical"] = {
            # 'storage_capacity': 'tidb_store_capacity{job="tidb"}',
            'tidb_cpu_usage': 'irate(process_cpu_seconds_total{k8s_cluster="", tidb_cluster="{0}", instance=~".*", job="tidb"}[30s])'.format(cluster_id),
            'tidb_memory_usage': 'process_resident_memory_bytes{k8s_cluster="", tidb_cluster="{0}", instance=~".*", job="tidb"}'.format(cluster_id),
            'tikv_cpu': 'count(node_cpu_seconds_total{mode="user", instance=~"db-tikv-.*"}) by (instance)',
            'tikv_memory': 'node_memory_MemTotal_bytes{component="tikv"}',
            'tikv_storage': 'sum(tikv_store_size_bytes{type="capacity"}) by (instance)',
            'pd_cpu': 'count(node_cpu_seconds_total{mode="user", instance=~"db-pd-.*"}) by (instance)',
            'pd_memory': 'node_memory_MemTotal_bytes{component="pd"}',
            'tiflash_cpu': 'count(node_cpu_seconds_total{mode="user", instance=~"db-tiflash-.*"}) by (instance)',
            'tiflash_memory': 'node_memory_MemTotal_bytes{component="tiflash"}',
            'tiflash_storage': 'sum(tiflash_system_current_metric_StoreSizeCapacity) by (instance)'
        }

        queries["soft"] = {
            'qps': 'sum(rate(tidb_executor_statement_total[1m]))',
            'error_rate': 'rate(tidb_server_query_duration_seconds_count{status="fail"}[5m])',
            'uptime': 'uptime_seconds{job="tidb"}',
            'storage_capacity': 'tidb_store_capacity{job="tidb"}',
            'tidb_cpu': 'count(node_cpu_seconds_total{mode="user", instance=~"db-tidb-.*"}) by (instance)',
            'tidb_memory': 'node_memory_MemTotal_bytes{component="tidb"}',
            'tikv_cpu': 'count(node_cpu_seconds_total{mode="user", instance=~"db-tikv-.*"}) by (instance)',
            'tikv_memory': 'node_memory_MemTotal_bytes{component="tikv"}',
            'tikv_storage': 'sum(tikv_store_size_bytes{type="capacity"}) by (instance)',
            'pd_cpu': 'count(node_cpu_seconds_total{mode="user", instance=~"db-pd-.*"}) by (instance)',
            'pd_memory': 'node_memory_MemTotal_bytes{component="pd"}',
            'tiflash_cpu': 'count(node_cpu_seconds_total{mode="user", instance=~"db-tiflash-.*"}) by (instance)',
            'tiflash_memory': 'node_memory_MemTotal_bytes{component="tiflash"}',
            'tiflash_storage': 'sum(tiflash_system_current_metric_StoreSizeCapacity) by (instance)'
        }

        return queries

    def get_raw_data(self, query, start_time, end_time, step):
        """
        获取原始数据
        :param query: Prometheus 查询语句
        :param start_time: 查询的开始时间（Unix 时间戳）
        :param end_time: 查询的结束时间（Unix 时间戳）
        :param step: 查询的时间间隔
        :return: 原始数据的 DataFrame
        """
        return self.get_custom_metric(query, start_time, end_time, step)

    def capacity_assessment(self, data):
        """
        容量评估，检查 TiDB 集群的负载情况
        :param data: 监控数据的 DataFrame
        :return: 容量评估结果（示例：是否需要扩展资源）
        """
        # 示例：假设 QPS > 1000 时需要扩展资源
        if data['value'].mean() > 1000:
            return "Capacity Warning: Need to scale up resources"
        else:
            return "Capacity is within limits"

    def inspection(self):
        """
        TiDB 集群巡检，检查系统是否出现异常
        :param data: 监控数据的 DataFrame
        :return: 巡检结果
        """
        operations = ["max", "average", "percentile_50", "percentile_75", "percentile_80", "percentile_85",
                        "percentile_90", "percentile_95", "percentile_99", "percentile_99.9"]
        data=self.prome_client.get_metric_aggregation_promotion(query=self.queries["qps"],start_time=self.conf["start_time"],end_time=self.conf["end_time"],step=self.conf["step_in_seconds"],operations=operations)
        print(data)