SET @FLINK_LABEL="flink-1.16.2";

SET @FLINK_ALL=CONCAT('*-*,',@FLINK_LABEL);
SET @FLINK_IDE=CONCAT('*-IDE,',@FLINK_LABEL);
SET @FLINK_NODE=CONCAT('*-nodeexecution,',@FLINK_LABEL);
SET @FLINK_STREAMIS=CONCAT('*-Streamis,',@FLINK_LABEL);

INSERT INTO `linkis_ps_configuration_config_key` (`key`, `description`, `name`, `default_value`, `validate_type`, `validate_range`, `engine_conn_type`, `is_hidden`, `is_advanced`, `level`, `treeName`) VALUES ('flink.client.memory', '取值范围：1-15，单位：G', 'flink client内存大小', '1g', 'Regex', '^([1-9]|1[0-5])(G|g)$', 'flink', '0', '0', '1', 'flink资源设置');
INSERT INTO `linkis_ps_configuration_config_key` (`key`, `description`, `name`, `default_value`, `validate_type`, `validate_range`, `engine_conn_type`, `is_hidden`, `is_advanced`, `level`, `treeName`) VALUES ('flink.client.cores', '取值范围：1-4，单位：个', 'flink client核心个数', '1', 'NumInterval', '[1,4]', 'flink', '0', '0', '1', 'flink资源设置');
INSERT INTO `linkis_ps_configuration_config_key` (`key`, `description`, `name`, `default_value`, `validate_type`, `validate_range`, `engine_conn_type`, `is_hidden`, `is_advanced`, `level`, `treeName`) VALUES ('flink.jobmanager.memory', '取值范围：1-15，单位：G', 'flink jobmanager内存大小', '1g', 'Regex', '^([1-9]|1[0-5])(G|g)$', 'flink', '0', '0', '2', 'flink资源设置');
INSERT INTO `linkis_ps_configuration_config_key` (`key`, `description`, `name`, `default_value`, `validate_type`, `validate_range`, `engine_conn_type`, `is_hidden`, `is_advanced`, `level`, `treeName`) VALUES ('flink.taskmanager.memory', '取值范围：1-15，单位：G', 'flink taskmanager内存大小', '4g', 'Regex', '^([1-9]|1[0-5])(G|g)$', 'flink', '0', '0', '3', 'flink资源设置');
INSERT INTO `linkis_ps_configuration_config_key` (`key`, `description`, `name`, `default_value`, `validate_type`, `validate_range`, `engine_conn_type`, `is_hidden`, `is_advanced`, `level`, `treeName`) VALUES ('flink.taskmanager.numberOfTaskSlots', '取值范围：1-4，单位：个', 'flink taskslots个数', '2', 'NumInterval', '[1,4]', 'flink', '0', '0', '3', 'flink资源设置');
INSERT INTO `linkis_ps_configuration_config_key` (`key`, `description`, `name`, `default_value`, `validate_type`, `validate_range`, `engine_conn_type`, `is_hidden`, `is_advanced`, `level`, `treeName`) VALUES ('flink.taskmanager.cpu.cores', '取值范围：1-8，单位：个', 'flink taskmanager核心个数', '2', 'NumInterval', '[1,8]', 'flink', '0', '0', '3', 'flink资源设置');
INSERT INTO `linkis_ps_configuration_config_key` (`key`, `description`, `name`, `default_value`, `validate_type`, `validate_range`, `engine_conn_type`, `is_hidden`, `is_advanced`, `level`, `treeName`) VALUES ('flink.container.num', '取值范围：1-4，单位：个', 'flink container个数', '2', 'NumInterval', '[1,4]', 'flink', '0', '0', '4', 'flink资源设置');
INSERT INTO `linkis_ps_configuration_config_key` (`key`, `description`, `name`, `default_value`, `validate_type`, `validate_range`, `engine_conn_type`, `is_hidden`, `is_advanced`, `level`, `treeName`) VALUES ('wds.linkis.engineconn.flink.app.parallelism', '取值范围：1-20，单位：个', 'flink并行度', '4', 'NumInterval', '[1,20]', 'flink', '0', '0', '4', 'flink资源设置');

insert into `linkis_ps_configuration_key_engine_relation` (`config_key_id`, `engine_type_label_id`)
    (select config.id as `config_key_id`, label.id AS `engine_type_label_id` FROM linkis_ps_configuration_config_key config
                                                                                      INNER JOIN linkis_cg_manager_label label ON config.engine_conn_type = 'flink' and label.label_value = @FLINK_ALL);

insert into `linkis_ps_configuration_config_value` (`config_key_id`, `config_value`, `config_label_id`)
    (select `relation`.`config_key_id` AS `config_key_id`, '' AS `config_value`, `relation`.`engine_type_label_id` AS `config_label_id` FROM linkis_ps_configuration_key_engine_relation relation
                                                                                                                                                 INNER JOIN linkis_cg_manager_label label ON relation.engine_type_label_id = label.id AND label.label_value = @FLINK_ALL);