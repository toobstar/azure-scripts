# Azure Scripts

## [PowerBI Capacity Scaler](power-bi-capacity-monitor.py)

Delivering BI dashboards to a customer or external party via PowerBI is a great model but infrastructure and hosting models are not simple:

1) Embedded (hosted by Azure) in units of $750 USD / month (per 1 CPU & 3GB RAM).  Starts cheap (for not much) but has a significant premium cost over the equivalent raw compute.
2) PowerBI Premium (hosted by Azure).  Annual commitments, more flexibility & likely same or more cost. 
3) Power BI Report Server (self hosted).  Pricing roughly $5k USD /month for license + compute.

Embedded is an appealing setup to start with but due to the cost at anything beyond minimal capacity you really need an auto-scale function which is not available by default.  This script solves that with a per-minute Azure Function call check to load and scale up/down as required.

More info
- https://learn.microsoft.com/en-us/power-bi/developer/embedded/embedded-performance-best-practices
- https://learn.microsoft.com/en-us/power-bi/developer/embedded/embedded-capacity-planning
