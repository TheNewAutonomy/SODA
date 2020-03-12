# SODA

UrbanWater is a project developed under the European Commission to enable water utilities to manage their water supply networks in smarter ways.

https://cordis.europa.eu/project/id/318602
http://urbanwater-ict.eu/

Data is collected either as messages posted to a service bus or as blobs uploaded by Data Concentrators (DCs) to Azure blob storage.
Worker roles then unpack, process and store submitted data to Azure Tables and SQL Azure storage while stored procedures perform data analytics on the stored data.

Partners of the water utilities can make OData calls to retrieve data.

The platform has successfully been run by water utilities in the following countries.

* Spain
* Portugal
* UK
* Czech Republic

Third party systems that have been developed on top of this platform include the following.

* Water demand prediction system
* Water supply prediction system
* Smart leak detection system
* Adaptive pricing system
* Automatic billing system
* Spatial decision support system
* Gamification system by an EU based games maker
* Smart meters by Sagemcom
* Management dashboard


SODA is a continuation of the UrbanWater platform to include other smart city use cases such as management of street lighting.
SODA is fully compatible with UrbanWater deployments but is open for further extension into other smart city domains.
