defmodule Electric.Postgres.Repo do
  use Ecto.Repo, otp_app: :electric, adapter: Ecto.Adapters.Postgres

  alias Electric.Replication.Connectors

  @default_pool_size 10

  def config(connector_config, opts) do
    conn_opts = Connectors.get_connection_opts(connector_config)

    [
      name: name(Connectors.origin(connector_config)),
      hostname: conn_opts.host,
      port: conn_opts.port,
      username: conn_opts.username,
      password: conn_opts.password,
      database: conn_opts.database,
      ssl: conn_opts.ssl == :required,
      pool_size: Keyword.get(opts, :pool_size, @default_pool_size),
      log: false
    ]
  end

  def name(origin), do: :"#{inspect(__MODULE__)}:#{origin}"
end
