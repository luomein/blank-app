import json 


def get_templates_by_interface(selected_interface):
    if selected_interface == 'sata':
        return [1] + [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 17, 18, 201, 202, 203]
    if selected_interface == 'nvme':
        return [101,102,103,104,105,201,202,203]
    if selected_interface == 'scsi':
        return [301,
 302,
 303,
 304,
 306,
 307,
 309,
 310,
 311,
 313,
 314,
 315,
 327,
 328,
 330,
 331,
 332,
 333,
 334] + [201, 202, 203]
        

def get_query_string(selected_interface , selected_template , selected_date , record_limit = 1 , select_feature = None
                    , pd_slot_activated = 'true' ):
    selected_template = int(selected_template)
    #if selected_template < 100:
    #    selected_interface = 'sata'
    #elif selected_template < 200:
    #    selected_interface = 'nvme'
    #elif     selected_template < 300:
    #     selected_interface = 'sata'
    #elif         selected_template < 400:
    #    selected_interface = 'scsi'
    #else:
    #    assert False , f"Template {selected_template} is not supported."
        
    script = f'''
    select idfy_serial_number , idfy_model_number 
    , '{selected_interface}' as interface , pd_records as templates
    , year , month , day
    , year || '-' || month || '-' || day as date
    
    , pd_slot_activated
    from da_{selected_interface}_parquet
    where year || month || day = '{selected_date}'
    and contains(pd_records , {selected_template})
    { ' and ' + select_feature if select_feature is not None  else ''}
    and pd_slot_activated = '{pd_slot_activated}'
    limit {record_limit}
    '''
    return script
    ## { ',' + select_feature.split(' ')[0] if select_feature is not None else '' }


def get_filtered_feature(selected_template):
    feature_json_string = """{
     "3": [
    "device_error_count_3_1"
  ],
  "4": [
    "lifetime_power_on_reset_4_1",
    "ds_power_on_hours_4_2",
    "logical_sectors_written_4_3",
    "number_of_write_commands_4_4",
    "logical_sectors_read_4_5",
    "number_of_read_commands_4_6",
    "pending_error_count_4_8",
    "workload_utilization_4_9",
    "utilization_usage_rate_4_10",
    "fraction_of_device_resources_available_4_11",
    "random_write_resources_used_4_12"
  ],
  "5": [
    "number_of_free_fall_events_detected_5_1",
    "number_of_shock_events_detected_5_2"
  ],
  "6": [
    "spindle_motor_power_on_hours_6_1",
    "head_flying_hours_6_2",
    "head_load_events_6_3",
    "reallocated_logical_sectors_6_4",
    "read_recovery_attempts_6_5",
    "number_of_mechanical_failures_6_6",
    "number_of_reallocation_candidate_logical_sectors_6_7",
    "number_of_high_priority_unload_events_6_8"
  ],
  "7": [
    "number_of_reported_uncorrectable_errors_7_1",
    "number_of_resets_between_command_acceptance_and_command_completion_7_2",
    "physical_element_status_changed_7_3"
  ],
  "8": [
    "current_temperature_8_1",
    "highest_average_short_term_temp_8_6",
    "lowest_average_short_term_temp_8_7",
    "time_in_over_temperature_8_10",
    "time_in_under_temperature_8_12"
  ],
  "9": [
    "hardware_resets_9_1",
    "asr_events_9_2",
    "interface_crc_errors_9_3"
  ],
  "10": [
    "percentage_used_10_1"
  ],
  "11": [
    "summary_smart_device_error_count_11_1"
  ],
  "12": [
    "comprehensive_smart_error_device_error_count_12_1"
  ],
  "14": [
    "self_test_log_list_14_2"
  ],
  "15": [
    "self_test_log_list_15_2"
  ],
  "16": [
    "interface_crc_error_count_16_1",
    "uncorrectable_error_count_16_2",
    "id_not_found_count_16_3",
    "command_aborted_count_16_4",
    "address_mark_not_found_16_5",
    "crc_error_count_16_7",
    "disparity_error_count_16_8",
    "b10_to_b8_decode_error_count_16_9",
    "internal_error_count_16_10",
    "protocol_error_count_16_11",
    "persistent_communication_error_count_16_12",
    "transient_data_integrity_error_count_16_13",
    "recovered_communications_error_count_16_14",
    "recovered_data_integrity_error_count_16_15",
    "handshake_error_count_16_6"
  ],
  "17": [
    "accessible_capacity_17_1",
    "logical_sectors_per_physical_sector_as_pwr_of_two_17_2",
    "logical_sector_offset_17_3",
    "logical_sector_size_17_4",
    "nominal_buffer_size_17_5"
  ],
  "18": [
    "zoned_field_18_1"
  ],
  "101": [
    "pci_vendor_id_101_1",
    "pci_subsystem_vendor_id_101_2",
    "version_101_6",
    "error_log_page_entries_101_7",
    "warning_composite_temperature_threshold_101_8",
    "critical_composite_temperature_threshold_101_9",
    "total_nvm_capacity_101_10"
  ],
  "102": [
    "namespace_size_102_1",
    "formatted_lba_size_102_2",
  
    "data_protection_capability_supportsprotectioninfotype1_102_3_0",
    "data_protection_capability_supportsprotectioninfotype2_102_3_1",
    "data_protection_capability_supportsprotectioninfotype3_102_3_2",
    "data_protection_capability_supportsfirsteightbytesinfo_102_3_3",
    "data_protection_capability_supportslasteightbytesinfo_102_3_4"
  ],
  "103": [
    "composite_temperature_103_2",
    "available_spare_103_3",
    "available_spare_threshold_103_4",
    "percentage_used_103_5",
    "logical_sectors_read_103_6",
    "logical_sectors_written_103_7",
    "number_of_read_commands_103_8",
    "number_of_write_commands_103_9",
    "controller_busy_time_103_10",
    "power_cycle_count_103_11",
    "power_on_hours_103_12",
    "unsafe_shutdowns_103_13",
    "media_and_data_integrity_errors_103_14",
    "error_information_log_entries_103_15",
    "warning_composite_temperature_time_103_16",
    "critical_composite_temperature_time_103_17",
    "temperature_sensor1_103_18",
    "temperature_sensor2_103_19",
    "temperature_sensor3_103_20",
    "temperature_sensor4_103_21",
    "temperature_sensor5_103_22",
    "temperature_sensor6_103_23",
    "temperature_sensor7_103_24",
    "temperature_sensor8_103_25",
  
    "critical_warning_spare_capacity_below_threshold_103_5_1_0",
    "critical_warning_temperature_above_or_below_threshold_103_5_1_1",
    "critical_warning_reliability_degraded_103_5_1_2",
    "critical_warning_read_only_mode_103_5_1_3",
    "critical_warning_volatile_memory_backup_device_failed_103_5_1_4"
  ],
  "104": [
    "error_log_namespace_distinct_104_1",
    "error_log_lba_distinct_104_1",
    "error_log_byte_dot_bit_parameter_error_location_104_1",
    "error_log_status_field_distinct_104_1",
    "error_log_command_id_distinct_104_1",
    "error_log_submission_queue_id_104_1",
    "error_log_error_count_104_1"
  ],
  "105": [
    "current_device_self_test_operation_status_105_1",
    "self_test_number_of_self_test_error_logs_105_3",
    "self_test_power_on_hours_distinct_105_4",
    "self_test_namespace_identifier_distinct_105_4",
    "self_test_status_code_distinct_105_4",
    "self_test_status_code_type_distinct_105_4",
    "self_test_failing_lba_distinct_105_4"
  ],
  "301": [
    "write_errors_corrected_without_substantial_delay_301_1",
    "write_errors_corrected_with_possible_delays_301_2",
    "total_write_error_counter_301_3",
    "write_total_errors_corrected_301_4",
    "write_total_times_correction_algorithm_processed_301_5",
    "write_total_bytes_processed_301_6",
    "total_write_uncorrected_errors_301_7"
  ],
  "302": [
    "read_errors_corrected_without_substantial_delay_302_1",
    "read_errors_corrected_with_possible_delays_302_2",
    "total_read_error_counter_302_3",
    "read_total_errors_corrected_302_4",
    "read_total_times_correction_algorithm_processed_302_5",
    "read_total_bytes_processed_302_6",
    "total_read_uncorrected_errors_302_7"
  ],
  "303": [
    "verify_errors_corrected_without_substantial_delay_303_1",
    "verify_errors_corrected_with_possible_delays_303_2",
    "total_verify_error_counter_303_3",
    "verify_total_errors_corrected_303_4",
    "verify_total_times_correction_algorithm_processed_303_5",
    "verify_total_bytes_processed_303_6",
    "total_verify_uncorrected_errors_303_7"
  ],
  "304": [
    "non_medium_error_count_304_1"
  ],
  "306": [
    "temperature_306_1",
    "reference_max_temperature_306_2"
  ],
  "307": [
    "manufacturing_week_307_1",
    "manufacturing_year_307_1",
    "accounting_date_week_307_2",
    "accounting_date_year_307_2",
    "specified_cycle_count_over_device_lifetime_307_3",
    "accumulated_start_stop_cycles_307_4",
    "specified_load_unload_count_over_device_lifetime_307_5",
    "accumulated_load_unload_cycles_307_6"
  ],
  "309": [
    "number_of_self_test_error_logs_309_1",
    "self_test_additional_sense_code_qualifier_309_2",
    "self_test_additional_sense_code_309_2",
    "self_test_sense_key_309_2",
    "self_test_accumulated_power_on_hours_309_2",
    "self_test_result_number_309_2",
    "self_test_result_code_309_2",
    "self_test_parameter_code_309_2"
  ],
  "310": [
    "percentage_used_endurance_indicator_310_1"
  ],
  "311": [
    "power_on_minutes_311_1",
    "background_scan_status_311_2",
    "number_of_background_scans_performed_311_3",
    "background_scan_progress_311_4",
    "no_background_medium_scans_performed_311_5",
    "background_scan_logical_block_address_311_6",
    "background_scan_vendor_specific_311_6",
    "background_scan_additional_sense_code_qualifier_311_6",
    "background_scan_additional_sense_code_311_6",
    "background_scan_sense_key_311_6",
    "background_scan_reassign_status_311_6"
  ],
  "313": [
    "protocol_specific_port_protocol_identifier_313_3",
    "protocol_specific_port_number_of_phys_313_4",
    "protocol_specific_port_invalid_dword_count_313_6",
    "protocol_specific_port_running_disparity_error_count_313_7",
    "protocol_specific_port_loss_of_dword_sync_count_313_8",
    "protocol_specific_port_phy_reset_problem_count_313_9"
  ],
  "314": [
    "number_of_read_commands_314_1",
    "number_of_write_commands_314_2",
    "number_of_logical_blocks_received_314_3",
    "number_of_logical_blocks_transmitted_314_4",
    "read_command_processing_intervals_max_314_5",
    "write_command_processing_intervals_max_314_6",
    "number_of_weighted_read_write_commands_314_7",
    "weighted_number_of_read_and_write_command_processing_314_8",
    "number_of_read_fua_commands_314_9",
    "number_of_write_fua_commands_314_10",
    "number_of_read_fua_nv_commands_314_11",
    "number_of_write_fua_nv_commands_314_12",
    "read_fua_command_processing_intervals_314_13",
    "write_fua_command_processing_intervals_314_14",
    "read_fua_nv_command_processing_intervals_314_15",
    "write_fua_nv_command_processing_intervals_314_16"
  ],
  "315": [
    "informational_exception_additional_sense_code_315_1",
    "informational_exception_additional_sense_code_qualifier_315_2",
    "informational_exception_temperature_reading_315_3"
  ],
  "327": [
    "maximum_compare_and_write_length_327_1",
    "optimal_transfer_length_granularity_327_2",
    "maximum_transfer_length_327_3",
    "optimal_transfer_length_327_4",
    "maximum_prefetch_length_327_5",
    "maximum_unmap_lba_count_327_6",
    "maximum_unmap_block_descriptor_count_327_7",
    "optimal_unmap_granularity_327_8",
    "unmap_granularity_alignment_327_9",
    "maximum_write_same_length_327_10"
  ],
  "328": [
    "nominal_form_factor_328_3"
  ],
  "330": [
    "read_capacity_10_returned_logical_block_address_330_1",
    "read_capacity_10_logical_block_length_in_bytes_330_2"
  ],
  "331": [
    "returned_logical_block_address_331_1",
    "logical_block_length_in_bytes_331_2",
    "read_capacity_16_protection_type_331_3",
    "read_capacity_16_protection_enabled_331_4",
    "read_capacity_16_number_of_protection_information_intervals_331_5",
    "logical_blocks_per_physical_block_exponent_331_6",
    "lowest_aligned_logical_block_address_331_7"
  ],
  "332": [
    "glist_generation_code_332_1",
    "read_grown_defect_list_length_332_3",
    "number_of_growing_defects_byte_based_332_4",
    "number_of_growing_defects_list_based_332_4"
  ],
  "333": [
    "plist_generation_code_333_1",
    "read_primary_defect_list_length_333_3",
    "number_of_primary_defects_list_based_333_4"
  ],
  "334": [
    "peripheral_qualifier_334_1",
    "peripheral_device_type_334_2",
    "inquiry_information_version_334_5",
    "protect_334_6",
    "multi_port_334_7",
    "t10_vendor_identification_334_8",
    "inquiry_information_vendor_specific_334_11"
  ],
  "201": [
    "write_retry_count_201_1",
    "read_retry_count_201_2",
    "other_command_retry_count_201_3",
    "read_iops_201_4",
    "write_iops_201_5",
    "long_latency_write_count_201_6",
    "long_latency_read_count_201_7",
    "read_mbps_201_8",
    "write_mbps_201_9",
    "hsw_command_timeout_count_201_10"
  ],
  "202": [
    "drive_not_ready_failure_202_1",
    "drive_write_operation_failure_202_2",
    "drive_read_operation_failure_202_3",
    "identify_failure_202_4",
    "other_drive_failure_202_5"
  ],
  "203": [
    "raid_group_id_203_1",
    "raid_type_203_2",
    "raid_capacity_203_3",
    "raid_status_203_4"
  ]
}"""

    features = json.loads(feature_json_string)
    if f"{selected_template}" in features:
        filtered_features = features[f"{selected_template}"]
        return [f" {f} > 0 " for f in filtered_features]
        #return f' {features[f"{selected_template}"]} > 0 '
    selected_template = int(selected_template)
    
    if selected_template == 9:
        return  ["hardware_resets_9_1 > 0", "asr_events_9_2 > 0"]
    else:
        return []
    return 