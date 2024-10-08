# Databricks notebook source
# MAGIC %run ./EnergyTrendAnalysis

# COMMAND ----------

import pytest
from unittest.mock import patch, mock_open, Mock
from requests.exceptions import RequestException
import pandas as pd
import os

# Test retry_request decorator
@patch('time.sleep', return_value=None)  # Mock sleep to speed up tests
def test_retry_request(mock_sleep):
    mock_func = Mock(side_effect=RequestException("Test exception"))

    @retry_request(max_retries=3, delay=1, backoff=2)
    def flaky_func():
        return mock_func()

    with pytest.raises(Exception, match="Failed to complete after 3 retries."):
        flaky_func()

    assert mock_func.call_count == 3  # Ensuring it retries the correct number of times

# Test download_excel_file function
@patch('requests.get')
@patch('dbutils.fs.cp')  # Mocking dbutils file system
@patch('builtins.open', new_callable=mock_open)  # Mocking file open function
def test_download_excel_file(mock_open_func, mock_cp, mock_get):
    mock_response = Mock()
    mock_response.iter_content = Mock(return_value=[b'file content'])
    mock_response.raise_for_status = Mock()
    mock_get.return_value = mock_response

    download_excel_file('https://example.com/file.xlsx', '/dbfs/destination', 'test.xlsx')

    mock_get.assert_called_once_with('https://example.com/file.xlsx', stream=True, timeout=10)
    mock_open_func.assert_called_once_with('/tmp/test.xlsx', 'wb')
    mock_cp.assert_called_once_with('file:/tmp/test.xlsx', '/dbfs/destination/test.xlsx')

# Test quarter_to_tuple function
def test_quarter_to_tuple():
    assert quarter_to_tuple('2023 1st quarter') == (2023, 1)
    assert quarter_to_tuple('2024 2nd quarter') == (2024, 2)
    with pytest.raises(ValueError):
        quarter_to_tuple('invalid quarter')

# Test check_excel_for_latest_quarter function
@patch('pandas.read_excel')
@patch('os.remove')  # Mock os.remove for cleanup
@patch('EnergyTrendAnalysis.download_excel_file')  # Mocking download_excel_file
def test_check_excel_for_latest_quarter(mock_download, mock_remove, mock_read_excel):
    df_mock = pd.DataFrame({
        '2023 1st quarter': [100],
        '2024 2nd quarter': [150]
    })
    mock_read_excel.return_value = df_mock

    should_download, latest_quarter = check_excel_for_latest_quarter('https://example.com/file.xlsx', '2023 1st quarter', '/tmp', 'test.xlsx')

    assert should_download is True
    assert latest_quarter == '2024 2nd quarter'
    mock_remove.assert_called_once_with('/tmp/test.xlsx')

# Test search_for_energy_trend function
@patch('requests.get')
@patch('EnergyTrendAnalysis.check_excel_for_latest_quarter')
@patch('EnergyTrendAnalysis.download_excel_file')
def test_search_for_energy_trend(mock_download_excel, mock_check_excel, mock_requests_get):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.content = b'<a href="/path/to/file.xlsx">Download Excel</a>'
    mock_requests_get.return_value = mock_response

    mock_check_excel.return_value = (True, '2024 1st quarter')

    search_for_energy_trend('https://www.gov.uk', 'Supply and use of crude oil', '2023 1st quarter', '/dbfs/destination')

    mock_requests_get.assert_called_once_with('https://www.gov.uk')
    mock_check_excel.assert_called_once_with('https://www.gov.uk/path/to/file.xlsx', '2023 1st quarter', '/dbfs/destination', 'file.xlsx')
    mock_download_excel.assert_called_once_with('https://www.gov.uk/path/to/file.xlsx', '/dbfs/destination', 'file.xlsx')

# Test search_for_energy_trend with no Excel link found
@patch('requests.get')
def test_search_for_energy_trend_no_excel_link(mock_requests_get):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.content = b'<html>No Excel files here</html>'
    mock_requests_get.return_value = mock_response

    search_for_energy_trend('https://www.gov.uk', 'Supply and use of crude oil', '2023 1st quarter', '/dbfs/destination')

    mock_requests_get.assert_called_once_with('https://www.gov.uk')
    # Ensure no download is attempted
    assert "No Excel file link found." in capsys.readouterr().out



# COMMAND ----------

pytest.main(["-v"])
