<!-- default badges list -->
![](https://img.shields.io/endpoint?url=https://codecentral.devexpress.com/api/v1/VersionRange/134371792/19.2.7%2B)
[![](https://img.shields.io/badge/Open_in_DevExpress_Support_Center-FF7200?style=flat-square&logo=DevExpress&logoColor=white)](https://supportcenter.devexpress.com/ticket/details/T830590)
[![](https://img.shields.io/badge/📖_How_to_use_DevExpress_Examples-e9f6fc?style=flat-square)](https://docs.devexpress.com/GeneralInformation/403183)
<!-- default badges end -->
# How to implement a custom XPO connection provider for AdoNetCore.AseClient

This example demonstrates how to implement a custom XPO connection provider for [AdoNetCore.AseClient](https://github.com/DataAction/AdoNetCore.AseClient) - a .NET Core DB Provider for SAP ASE.

The example contains two projects:
* DevExpress.Xpo.AseClientCore - .NET Standard 2.0 library with the provider.
* TestingConsoleProject - .NET Core 3.1 Console Application, which uses [XPO for .NET Core](https://www.devexpress.com/Products/NET/ORM/) and AdoNetCore.AseClient to connect to the SAP ASE database.

# See Also
[Database Systems Supported by XPO](https://documentation.devexpress.com/CoreLibraries/2114/DevExpress-ORM-Tool/Fundamentals/Database-Systems-Supported-by-XPO)<br>
[How to: Connect to a Data Store](https://documentation.devexpress.com/CoreLibraries/2123/DevExpress-ORM-Tool/Concepts/How-to-Connect-to-a-Data-Store)
