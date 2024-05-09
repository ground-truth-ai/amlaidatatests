### Using the starter project

Set your db connections in ~/.dbt/profiles.yml

Running tests on the target db, e.g.:
- dbt test --target postgres 
- dbt test --target postgres --select "tag:excessive_validity_start_time_changes"
- dbt test --target bigqueryß


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
