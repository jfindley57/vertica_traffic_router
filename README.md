# Vertica Traffic Router
## Requirements
- Two or more Vertica clusters with LIKE datasets
- Mysql database with latest_loaded date of each Vertica cluster

## How the proxy works
- The proxy works by determining which Vertica cluster to route traffic to based upon several factors
- Those factors are:
  - Memory Usage
  - Cpu Usage
  - Node availability (If nodes are down the cluster is de-prioritized)
  - Data freshness
  - Preferred Cluster (configurable)
- A Weight will be determined based upon the factors above. The cluster with the lowest weight value will be prioritized for traffic
- Typically Data freshness has the biggest weight penalty as to direct traffic to the cluster with the newest data

## Example dictionary output
```
{'Results': [('internal-vertica-c.us-west-1.elb.amazonaws.com', {'weight': 100, 'mem_penalty': 0, 'mem_usage': 0, 'state': 'UP', 'latest_loaded': 60, 'usage': 40}), ('internal-vertica-b.us-east-1.elb.amazonaws.com', {'weight': 115, 'mem_penalty': 0, 'mem_usage': 5, 'state': 'UP', 'preferred': True, 'latest_loaded': 120, 'usage': 20})]}
```