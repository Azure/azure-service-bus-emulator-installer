# syntax=docker/dockerfile:1

# ---- Build stage ----
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Restore first to leverage layer caching.
COPY ServiceBusExplorer.Spa.csproj ./
RUN dotnet restore ServiceBusExplorer.Spa.csproj

# Copy the rest of the source and publish.
COPY . ./
RUN dotnet publish ServiceBusExplorer.Spa.csproj \
    -c Release \
    -o /app/publish \
    --no-restore \
    /p:UseAppHost=false

# ---- Runtime stage ----
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app

# Run as the non-root user provided by the base image.
USER $APP_UID

ENV ASPNETCORE_HTTP_PORTS=9010
EXPOSE 9010

COPY --from=build /app/publish ./

ENTRYPOINT ["dotnet", "ServiceBusExplorer.Spa.dll"]
