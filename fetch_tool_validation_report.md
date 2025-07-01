# Fetch Tool Validation Report

The test suite verifies the behaviour of the contract downloader and related utilities.

```
tests/test_contract_downloader.py::test_prepend_and_limit PASSED         [  3%]
tests/test_contract_downloader.py::test_update_contract_store_two_stage PASSED [  7%]
tests/test_contract_downloader.py::test_update_contract_store_two_stage_rpc PASSED [ 11%]
tests/test_contract_downloader.py::test_update_contract_store_default_latest PASSED [ 34%]
tests/test_contract_stats.py::test_collect_contract_addresses PASSED     [ 38%]
tests/test_contract_stats.py::test_count_contracts PASSED                [ 42%]
tests/test_contract_stats.py::test_file_data_store PASSED                [ 46%]
tests/test_contract_stats.py::test_get_current_block_number PASSED       [ 50%]
tests/test_fetch_and_check.py::test_random_contract_address PASSED       [ 53%]
tests/test_fetch_and_check.py::test_run_checks_calls PASSED              [ 57%]
tests/test_fetch_and_check.py::test_get_provider_url_mainnet PASSED      [ 61%]
tests/test_fetch_and_check.py::test_get_provider_url_invalid PASSED      [ 65%]
tests/test_fetch_and_check.py::test_scan_multiple_contracts_unique PASSED [ 69%]
tests/test_fetch_and_check.py::test_scan_multiple_contracts_allow_duplicates PASSED [ 73%]
tests/test_fetch_and_check.py::test_scan_multiple_contracts_respects_existing_file PASSED [ 76%]
tests/test_fetch_and_check.py::test_vulnerable_addresses_written PASSED  [ 80%]
tests/test_fetch_and_check_extra.py::test_fetch_contract_bytecode_returns_hex PASSED [ 84%]
tests/test_fetch_and_check_extra.py::test_random_contract_address_error PASSED [ 88%]
```

All tests passed.
