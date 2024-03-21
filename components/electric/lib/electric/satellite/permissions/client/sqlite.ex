defmodule Electric.Satellite.Permissions.Client.SQLite do
  alias Electric.Replication.Eval

  import Electric.Satellite.Permissions.Client.Format

  def create_trigger(args) do
    name =
      Keyword.get_lazy(args, :name, fn ->
        trigger_name(args[:table], args[:event])
      end)

    lines([
      "--",
      "",
      "INSERT INTO #{table(triggers_and_functions_table())} (name, type) VALUES (#{val(name)}, 'trigger');",
      "",
      "CREATE TRIGGER #{quot(name)}",
      indent([
        join_optional([
          Keyword.get(args, :when, "BEFORE"),
          args[:event],
          optional(args[:of], fn of -> ["OF ", lst(of, &quot/1)] end),
          "ON",
          table(args[:table])
        ]),
        "FOR EACH ROW",
        optional(args[:condition], &prefix(&1, "WHEN "))
      ]),
      "BEGIN",
      indent(List.wrap(args[:body])),
      "END;",
      ""
    ])
  end

  def rollback(message) do
    "SELECT RAISE(ROLLBACK, #{val(message)});"
  end

  def table(table, quot \\ true)

  def table(table, true), do: table(table, false) |> quot()

  def table({"electric", table}, false), do: "__electric_#{table}"
  def table({_schema, table}, false), do: table
  def table(%{schema: _schema, name: table}, false), do: table

  def trigger_name(table, action, suffixes \\ []) do
    trigger_name(table, action, __MODULE__, suffixes)
  end

  def expr(%Eval.Expr{eval: expr}, values) do
    expr
    |> expr(values)
    |> IO.iodata_to_binary()
  end

  @quoted_infix ~w(+ - / * ^ & | > < >= <= = ||)
  @infix_funcs %{"and" => "AND", "or" => "OR"}
               |> Map.merge(Map.new(@quoted_infix, &{~s|"#{&1}"|, &1}))
  @infix_func_names Map.keys(@infix_funcs)

  def expr(%Eval.Parser.Func{name: name, args: [a1, a2]} = _func, values)
      when name in @infix_func_names do
    [arg_expr(a1, values), " ", @infix_funcs[name], " ", arg_expr(a2, values)]
  end

  # just remove casting of values
  def expr(%Eval.Parser.Func{cast: {_from, _to}, args: [a1]}, values) do
    expr(a1, values)
  end

  def expr(%Eval.Parser.Func{} = func, values) do
    [
      unquoted(func.name),
      "(",
      Enum.map(func.args, &expr(&1, values)) |> Enum.intersperse(", "),
      ")"
    ]
  end

  def expr(%Eval.Parser.Ref{} = ref, values) do
    ref.path
    |> normalise_ref(values)
    |> Enum.join(".")
  end

  def expr(%Eval.Parser.Const{type: :text} = const, _values) do
    sval(const.value)
  end

  def expr(%Eval.Parser.Const{} = const, _valuues) do
    to_string(const.value)
  end

  defp arg_expr(%Eval.Parser.Ref{} = ref, values) do
    expr(ref, values)
  end

  defp arg_expr(%Eval.Parser.Const{} = cons, valuest) do
    expr(cons, valuest)
  end

  defp arg_expr(%Eval.Parser.Func{args: [_]} = func, values) do
    expr(func, values)
  end

  defp arg_expr(exp, valuesr) do
    ["(", expr(exp, valuesr), ")"]
  end

  defp normalise_ref(["new" | rest], _values), do: ["NEW" | rest]
  defp normalise_ref(["old" | rest], _values), do: ["OLD" | rest]

  defp normalise_ref(ref, values) do
    case Map.fetch(values, ref) do
      {:ok, value} when is_binary(value) ->
        [sval(value)]

      {:ok, value} ->
        [to_string(value)]

      :error ->
        raise "Unknown reference #{inspect(ref)}"
    end
  end

  defp sval(s) do
    IO.iodata_to_binary(["'", :binary.replace(s, "'", "''", [:global]), "'"])
  end

  defp unquoted(<<?", rest::binary>>) do
    String.trim_trailing(rest, "\"")
  end

  defp unquoted(name), do: name
end
