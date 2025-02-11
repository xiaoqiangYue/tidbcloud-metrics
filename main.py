import sys
import datetime
from health_checker.health_checker import HealthChecker
from utils.logger import setup_logger
from utils import helpers
from tidb_cluster.configer import Configer
from capacity_planner.capacity_planner import CapacityPlanner
from tidb_cluster.tidb_cluster import TiDBCluster
from tidb_cluster.op_tidb_cluster import OpTidbCluster
import click
from lark.app import LarkApp
import utils
from tidb_cluster.init_op_conf import TidbOpConfig

# consts
health_check_type = ["all", "tidb", "tikv", "pd", "tiflash"]
talent_bz_dynamic = ["talent"]
tidb_inspection = ["all"]
#k8s_prom_url = "https://www.ds.us-east-1.aws.observability.tidbcloud.com/internal/metrics/d5d1a915-1d37-22a7-82b8-8cb67cc57820" # hardcode first


@click.group()
def cli():
    click.echo('Welcome to TiDBCloud Capacity Planner and Health Checker!')

@cli.command()
@click.option('--inspection', '-i', prompt=True, type=click.Choice(tidb_inspection), default='all', help='tidb inspection')
def inspection(inspection):
    # if inspection == 'all':
    #     click.confirm("Have you connected to FeiLian?", abort=True)
    # 1. 初始化 OP 配置
    conf = TidbOpConfig("tidbcloud.yaml").set_conf()
    # 2. 初始化 TiDBOP 对象，对象包含以下能力
    tidb_op_cluster = OpTidbCluster(conf)
    #   a. OPTiDB 定义 getAPI function 用来获取可以触达 prome url ,已不需要

    #   b. OPTiDB init 初始化通过 Prometheus API 获取 prome client 对象，已实现

    #   c. OPTiDB 定义 getCustomMetric 用来获取监控数据，已实现

    #   d. OPTiDB 定义 query，用来定义可以获取监控指标的种类，已实现

    #   e. OPTiDB 定义固定功能，用来获取最终数据
    # https://clinic.pingcap.com/clinic/api/v1/data/metrics?query=tidb_server_connections&start=1734937200&end=1734944400&step=60
    tidb_op_cluster.get_all_pd_instance()
    #   f. OPTiDB 定义的固定功能包括：获取原始数据，容量评估，巡检
    # 3. 

@cli.command()
@click.option('--business', '-b', prompt=True, type=click.Choice(talent_bz_dynamic), default='talent', help='business dynamic')
def cluster_info(business):
    if business == 'talent':
        click.confirm("Have you connected to FeiLian?", abort=True)
        # 1. 获取租户下所有的 clusters
    conf = Configer("tidbcloud.yaml").set_conf()
    tidb_cluster = TiDBCluster(conf)
    clusters = tidb_cluster.get_dedicated_clusters_by_tenant_from_k8s()
    clusters_instance_type = tidb_cluster.get_clusters_basic_info_by_tenant_from_k8s()
    # 2. 遍历 clusters，收集硬件指标(组件，对应机型，节点数)和 QPS

    # conf['capacity']['cluster_list']
    
    # end_time = datetime.datetime.now()
    # start_time = end_time - datetime.timedelta(days=1)

    end_time = helpers.convert_datetime(conf['prometheus']['end_time'])
    start_time = helpers.convert_datetime(conf['prometheus']['start_time'])
    operations=['max']
    clusters_qps_and_nodecount = tidb_cluster.get_basic_info_by_clusters(clusters,start_time,end_time,operations)

    # 3. 获取 clusters 集群名称，集群版本
    clusters_name_and_version = tidb_cluster.get_cluster_basic_info_by_ai()
    # print(clusters_name_and_version)
    # print(type(clusters_name_and_version))

    # 4. 将收集的数据汇总
    # clusters_name_and_version,clusters_instance_type,clusters_qps_and_nodecount
    # 合并 clusters_name_and_version 和 clusters_qps_and_nodecount 信息
    merge_qps_and_nodecount={
                    'PD 节点数':'pd',
                    'TiDB 节点数':'tidb',
                    'TiKV 节点数':'tikv',
                    'TiFlash 节点数':'tiflash',
                    'Total QPS(MAX)':'qps',
                    '实际数据存储量(byte)':'data_size'
                }
        
    merged_res=utils.helpers.merged_list_through_dict_key_value(clusters_name_and_version,clusters_qps_and_nodecount,"cluster_id",merge_qps_and_nodecount)
    # logger.info("first merged_res: {} ".format(merged_res))
    
    # 合并 merged_res 和 clusters_instance_type 信息

    merge_instance_type={
        '组件':'component',
        '实例类型':'component_instance_type',
        'CPU(core)': 'CPU(core)',
        'Memory(byte)': 'Memory(byte)',
    }
    merged_res=utils.helpers.merged_list_through_dict_key_value_and_combine_custom_kv(merged_res,clusters_instance_type,"cluster_id",merge_instance_type,True)
    # logger.info("second merged_res: {} ".format(merged_res))
    merged_res=utils.helpers.deduplicate_use_custom_key(merged_res,"cluster_id")
    # logger.info("third merged_res: {} ".format(merged_res))

    headers=['cluster_id', 'cluster_name', 'version', 'PD 节点数', 'TiDB 节点数', 'TiKV 节点数', 'TiFlash 节点数', 'Total QPS(MAX)', '实际数据存储量(byte)', '组件tikv', '组件tidb', '组件pd', '组件tiflash']

    
    merged_res=utils.helpers.keep_same_headers(merged_res,headers)
    # 打印合并后的结果


    utils.helpers.write_dictlist_to_csv(merged_res,tidb_cluster.csv_file_name)
    

@cli.command()
@click.option('--mode', '-m', prompt=True, type=click.Choice(['all', 'node', 'cluster']), default='all', help='capacity planner mode')
def capacity(mode):
    if mode == 'all' or mode == 'node':
        click.confirm("Have you connected to FeiLian?", abort=True)
    # 1. 遍历 cluster list
    # 2. 遍历 water list
    # 3. 每种组合赋值 conf 集群信息
    # 4. 调用 generate_capacity_plan
    # tidb_cluster = TiDBCluster(conf)
    conf = Configer("tidbcloud.yaml").set_conf()
    cluster_list=conf['capacity']['cluster_list']
    watermark_list=conf['capacity']['plan_resource_redundancy_x_list']
    for c in cluster_list:
        for w in watermark_list:
            conf['cluster_info']['cluster_id'] = c
            conf['capacity']['plan_resource_redundancy_x']=w
            capacity_planner = CapacityPlanner(conf)
            capacity_planner.generate_capacity_plan(mode)


@cli.command()
@click.option('--type', '-t', prompt=True, type=click.Choice(health_check_type), default='all', help='health check type')
@click.option('--report', '-r', prompt=True, type=click.Choice(['console']), default='console', help='report channel')
def health_check(type, report):
    conf = Configer("tidbcloud.yaml").set_conf()
    tidb_cluster = TiDBCluster(conf)
    if type == 'tiflash':
        if not tidb_cluster.validate_component(type):
            logger.info("Cluster {} doesn't have any {} instances.".format(conf['cluster_info']['cluster_id'], type))
            sys.exit(0)
    health_checker = HealthChecker(conf, health_check_type)
    health_checker.check_health(type, report)


@cli.command()
@click.option('--write', '-w', prompt=True, type=click.Choice(['Yes', 'No']), default='Yes', help='Write to spreadsheet')
def list_clusters(write):
    conf = Configer("tidbcloud.yaml").set_conf()
    tidb_cluster = TiDBCluster(conf)
    clusters = tidb_cluster.get_dedicated_clusters_by_tenant_from_k8s()
    components_list = [['TenantID', 'ProjectID', 'ClusterID', 'TiDB_Cnt', 'TiDB_CPU', 'TiDB_Memory(byte)', 'PD_Cnt', 'PD_CPU', 'PD_Memory(byte)', 'TiKV_Cnt', 'TiKV_CPU', 'TiKV_Memory(byte)', 'TiFlash_Cnt', 'TiFlash_CPU', 'TiFlash_Memory(byte)']]
    for cluster in clusters:
        conf['cluster_info'] = cluster
        new_cluster = TiDBCluster(conf)
        components = new_cluster.get_components_from_k8s()

        capacity = {}
        for component in components.keys():
            instances = new_cluster.get_instances_by_component(component)
            ec2_type = list(instances[0].values())[0]
            capacity[component] = new_cluster.get_capacity_by_instance_type(ec2_type)

        if 'tiflash' in components:
            components_node_count_list = [cluster['tenant_id'], cluster['project_id'], cluster['cluster_id'], int(components['tidb']), int(capacity['tidb']['CPU(core)']), int(capacity['tidb']['Memory(byte)']), int(components['pd']), int(capacity['pd']['CPU(core)']), int(capacity['pd']['Memory(byte)']), int(components['tikv']), int(capacity['tikv']['CPU(core)']), int(capacity['tikv']['Memory(byte)']), int(components['tiflash']), int(capacity['tiflash']['CPU(core)']), int(capacity['tiflash']['Memory(byte)'])]
            print("Cluster ID: {} Project ID: {} Tenant ID: {} TiDB Count: {} TiDB CPU: {} TiDB Memory: {} PD Count: {} PD CPU: {} PD Memory: {} TiKV Count: {} TiKV CPU: {} TiKV Memory:{} TiFlash Count: {} TiFlash CPU: {} TiFlash Memory: {}".format(cluster['cluster_id'], cluster['project_id'], cluster['tenant_id'], components['tidb'], capacity['tidb']['CPU(core)'], capacity['tidb']['Memory(byte)'], components['pd'], capacity['pd']['CPU(core)'], capacity['pd']['Memory(byte)'], components['tikv'], capacity['tikv']['CPU(core)'], capacity['tikv']['Memory(byte)'], components['tiflash'], capacity['tiflash']['CPU(core)'], capacity['tiflash']['Memory(byte)']))
        else:
            components_node_count_list = [cluster['tenant_id'], cluster['project_id'], cluster['cluster_id'], int(components['tidb']), int(capacity['tidb']['CPU(core)']), int(capacity['tidb']['Memory(byte)']), int(components['pd']), int(capacity['pd']['CPU(core)']), int(capacity['pd']['Memory(byte)']), int(components['tikv']), int(capacity['tikv']['CPU(core)']), int(capacity['tikv']['Memory(byte)']), 0, 0, 0]
            print("Cluster ID: {} Project ID: {} Tenant ID: {} TiDB Count: {} TiDB CPU: {} TiDB Memory: {} PD Count: {} PD CPU: {} PD Memory: {} TiKV Count: {} TiKV CPU: {} TiKV Memory:{}".format(cluster['cluster_id'],
                                                                                             cluster['project_id'],
                                                                                             cluster['tenant_id'],
                                                                                             components['tidb'],
                                                                                             capacity['tidb']['CPU(core)'],
                                                                                             capacity['tidb']['Memory(byte)'],
                                                                                             components['pd'],
                                                                                              capacity['pd']['CPU(core)'],
                                                                                             capacity['pd']['Memory(byte)'],
                                                                                             components['tikv'],
                                                                                            capacity['tikv']['CPU(core)'],
                                                                                            capacity['tikv']['Memory(byte)']
                                                                                             ))
        components_list.append(components_node_count_list)

    if write == 'Yes':
        components_list.append(["更新时间: {}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))])
        lark = LarkApp(conf)
        lark.write_sheet(components_list)


if __name__ == '__main__':
    # conf = Configer("tidbcloud.yaml").set_conf()
    # logger = setup_logger(__name__, conf['logging']['file_name'], conf['logging']['level'])
    cli()