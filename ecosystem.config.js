module.exports = {
  apps: [
    {
      name: "router-listener",
      script: "uv",
      args: "run python router_listener.py",
      interpreter: "none",
      cwd: __dirname,
    },
    {
      name: "router-api",
      script: "uv",
      args: "run fastapi run",
      interpreter: "none",
      cwd: __dirname,
    }
  ]
};
