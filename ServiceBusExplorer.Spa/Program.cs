using ServiceBusExplorer.AppConfiguration;

var builder = WebApplication.CreateBuilder(args);

var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles();

app.ConfigureEndpoints();

app.Run();
