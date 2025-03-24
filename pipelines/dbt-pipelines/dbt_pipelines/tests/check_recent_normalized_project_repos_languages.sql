-- tests/check_recent_normalized_project_repos_languages.sql
{
    { 
        check_no_recent_data(model_name=ref('normalized_project_repos_languages'), 
        column_name='data_timestamp', 
        days_ago=35) 
    }
}