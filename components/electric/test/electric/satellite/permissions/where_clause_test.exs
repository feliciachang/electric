defmodule Electric.Satellite.Permissions.WhereClauseTest do
  use ExUnit.Case, async: true

  alias ElectricTest.PermissionsHelpers
  alias ElectricTest.PermissionsHelpers.Chgs

  alias Electric.Satellite.Auth
  alias Electric.Satellite.Permissions
  alias Electric.Satellite.Permissions.Client

  @user_id "b2ce289a-3d2d-4ff7-9892-d446d5866f74"
  @not_user_id "ec61ba28-7195-47a2-8d93-e71068dc7160"
  @table {"public", "lotsoftypes"}

  setup do
    {:ok, schema_version} = PermissionsHelpers.Schema.load()
    auth = %Auth{user_id: @user_id}

    evaluator = Permissions.Eval.new(schema_version, auth)

    {:ok, auth: auth, schema_version: schema_version, evaluator: evaluator}
  end

  def expression(cxt, stmt) do
    assert {:ok, _expr_cxt} = Permissions.Eval.expression_context(cxt.evaluator, stmt, @table)
  end

  # execute the statement. if we pass `stmt` as a single-arity function
  # then it will be tested against all the valid prefixes for a generic
  # row, that is `this.`, `row.` and ``, this allows us to refer to
  # row columns as either `this.column`, `row.column` or just `column`
  # in where/if expressions.
  def execute(cxt, stmt, change) do
    if is_function(stmt) do
      results =
        for prefix <- ["ROW.", "THIS.", ""] do
          assert {:ok, expr_cxt} = expression(cxt, stmt.(prefix))
          assert {:ok, result} = Permissions.Eval.execute(expr_cxt, change)
          result
        end

      # make sure that all results are the same
      assert length(Enum.uniq(results)) == 1

      {:ok, hd(results)}
    else
      assert {:ok, expr_cxt} = expression(cxt, stmt)
      Permissions.Eval.execute(expr_cxt, change)
    end
  end

  def update(base, changes \\ %{}) do
    Chgs.update(@table, base, changes)
  end

  def insert(record) do
    Chgs.insert(@table, record)
  end

  def delete(record) do
    Chgs.delete(@table, record)
  end

  def change(f, r) do
    apply(__MODULE__, f, [r])
  end

  describe "UPDATE" do
    test "automatic casting when comparing auth", cxt do
      stmt = &"#{&1}user_id = AUTH.user_id"

      assert {:ok, true} = execute(cxt, stmt, update(%{"user_id" => @user_id}))
    end

    test "with NEW reference", cxt do
      stmt = "NEW.user_id::text = AUTH.user_id"

      assert {:ok, true} =
               execute(cxt, stmt, update(%{"user_id" => @not_user_id}, %{"user_id" => @user_id}))
    end

    test "with OLD reference", cxt do
      stmt = "OLD.user_id::text = auth.user_id"

      assert {:ok, true} =
               execute(cxt, stmt, update(%{"user_id" => @user_id}))

      assert {:ok, false} =
               execute(cxt, stmt, update(%{"user_id" => @not_user_id}, %{"user_id" => @user_id}))
    end

    test "with ROW reference", cxt do
      stmt = &"#{&1}user_id::text = auth.user_id"

      assert {:ok, true} =
               execute(cxt, stmt, update(%{"user_id" => @user_id}))

      assert {:ok, false} =
               execute(cxt, stmt, update(%{"user_id" => @not_user_id}, %{"user_id" => @user_id}))

      assert {:ok, false} =
               execute(cxt, stmt, update(%{"user_id" => @user_id}, %{"user_id" => @not_user_id}))
    end

    test "multi-clause ROW/THIS reference", cxt do
      stmt = &"(#{&1}user_id::text = auth.user_id) AND #{&1}valid"

      assert {:ok, true} =
               execute(
                 cxt,
                 stmt,
                 update(%{"user_id" => @user_id, "valid" => true})
               )

      assert {:ok, false} =
               execute(
                 cxt,
                 stmt,
                 update(%{"user_id" => @user_id, "valid" => false}, %{"valid" => true})
               )

      assert {:ok, false} =
               execute(
                 cxt,
                 stmt,
                 update(%{"user_id" => @not_user_id, "valid" => true}, %{"user_id" => @user_id})
               )
    end

    test "mixed row and NEW references", cxt do
      stmt = &"(#{&1}user_id::text = auth.user_id) AND NOT new.valid"

      assert {:ok, true} =
               execute(
                 cxt,
                 stmt,
                 update(%{"user_id" => @user_id, "valid" => true}, %{"valid" => false})
               )

      assert {:ok, false} =
               execute(
                 cxt,
                 stmt,
                 update(%{"user_id" => @user_id, "valid" => true})
               )

      assert {:ok, false} =
               execute(
                 cxt,
                 stmt,
                 update(%{"user_id" => @not_user_id, "valid" => true}, %{
                   "user_id" => @user_id,
                   "valid" => false
                 })
               )
    end

    test "with NEW reference to bool column", cxt do
      stmt = "new.valid"

      assert {:ok, true} =
               execute(cxt, stmt, update(%{"valid" => true}))

      assert {:ok, false} =
               execute(cxt, stmt, update(%{"valid" => false}))
    end

    test "with NOT(ROW) reference to bool column", cxt do
      stmt = &"NOT #{&1}valid"

      assert {:ok, false} =
               execute(cxt, stmt, update(%{"valid" => true}))

      assert {:ok, true} =
               execute(cxt, stmt, update(%{"valid" => false}))

      assert {:ok, false} =
               execute(cxt, stmt, update(%{"valid" => false}, %{"valid" => true}))
    end

    test "with THIS reference to bool column", cxt do
      stmt = &"#{&1}valid"

      assert {:ok, true} =
               execute(cxt, stmt, update(%{"valid" => true}))

      assert {:ok, false} =
               execute(cxt, stmt, update(%{"valid" => true}, %{"valid" => false}))

      assert {:ok, false} =
               execute(cxt, stmt, update(%{"valid" => false}, %{"valid" => true}))
    end
  end

  for {change_fun, name, ref} <- [
        {:insert, "INSERT", "NEW"},
        {:delete, "DELETE", "OLD"}
      ] do
    describe name do
      test "with #{ref} reference", cxt do
        stmt = "#{unquote(ref)}.user_id::text = AUTH.user_id"

        assert {:ok, true} =
                 execute(cxt, stmt, change(unquote(change_fun), %{"user_id" => @user_id}))

        assert {:ok, false} =
                 execute(cxt, stmt, change(unquote(change_fun), %{"user_id" => @not_user_id}))
      end

      test "with ROW reference", cxt do
        stmt = &"#{&1}user_id::text = auth.user_id"

        assert {:ok, true} =
                 execute(cxt, stmt, change(unquote(change_fun), %{"user_id" => @user_id}))

        assert {:ok, false} =
                 execute(cxt, stmt, change(unquote(change_fun), %{"user_id" => @not_user_id}))
      end

      test "multi-clause ROW/THIS reference", cxt do
        stmt = &"(#{&1}user_id::text = auth.user_id) AND #{&1}valid"

        assert {:ok, true} =
                 execute(
                   cxt,
                   stmt,
                   change(unquote(change_fun), %{"user_id" => @user_id, "valid" => true})
                 )

        assert {:ok, false} =
                 execute(
                   cxt,
                   stmt,
                   change(unquote(change_fun), %{"user_id" => @user_id, "valid" => false})
                 )

        assert {:ok, false} =
                 execute(
                   cxt,
                   stmt,
                   change(unquote(change_fun), %{"user_id" => @not_user_id, "valid" => true})
                 )
      end

      test "with #{ref} reference to bool column", cxt do
        stmt = "#{unquote(ref)}.valid"

        assert {:ok, true} =
                 execute(cxt, stmt, change(unquote(change_fun), %{"valid" => true}))

        assert {:ok, false} =
                 execute(cxt, stmt, change(unquote(change_fun), %{"valid" => false}))
      end

      test "with NOT(ROW) reference to bool column", cxt do
        stmt = &"NOT #{&1}valid"

        assert {:ok, false} =
                 execute(cxt, stmt, change(unquote(change_fun), %{"valid" => true}))

        assert {:ok, true} =
                 execute(cxt, stmt, change(unquote(change_fun), %{"valid" => false}))
      end

      test "with THIS reference to bool column", cxt do
        stmt = &"#{&1}valid"

        assert {:ok, true} =
                 execute(cxt, stmt, change(unquote(change_fun), %{"valid" => true}))

        assert {:ok, false} =
                 execute(cxt, stmt, change(unquote(change_fun), %{"valid" => false}))
      end
    end
  end

  describe "sqlite client representation" do
    setup do
      vals = %{
        ["auth", "user_id"] => "56f59cd3-566e-47bb-9c13-9dffdf6faa85"
      }

      {:ok, vals: vals}
    end

    def assert_client_expr(cxt, query, expected, action \\ :insert) do
      assert {:ok, expr} = expression(cxt, query)
      assert Client.sql_expr(expr, action, cxt.vals) == expected
    end

    test "simple boolean", cxt do
      assert {:ok, expr} = expression(cxt, "this.valid")
      assert Client.sql_expr(expr, :insert, cxt.vals) == "NEW.valid"
      assert Client.sql_expr(expr, :delete, cxt.vals) == "OLD.valid"
      assert Client.sql_expr(expr, :update, cxt.vals) == "NEW.valid AND OLD.valid"
    end

    test "a or b", cxt do
      assert {:ok, expr} = expression(cxt, "this.valid or this.value = 'important'")
      assert Client.sql_expr(expr, :insert, cxt.vals) == "NEW.valid OR (NEW.value = 'important')"
      assert Client.sql_expr(expr, :delete, cxt.vals) == "OLD.valid OR (OLD.value = 'important')"

      assert Client.sql_expr(expr, :update, cxt.vals) ==
               "(NEW.valid OR (NEW.value = 'important')) AND (OLD.valid OR (OLD.value = 'important'))"
    end

    test "constants", cxt do
      assert {:ok, expr} = expression(cxt, "this.amount = 4 or this.value = 'important'")

      assert Client.sql_expr(expr, :insert, cxt.vals) ==
               "(NEW.amount = 4) OR (NEW.value = 'important')"

      assert Client.sql_expr(expr, :delete, cxt.vals) ==
               "(OLD.amount = 4) OR (OLD.value = 'important')"

      assert Client.sql_expr(expr, :update, cxt.vals) ==
               "((NEW.amount = 4) OR (NEW.value = 'important')) AND ((OLD.amount = 4) OR (OLD.value = 'important'))"
    end

    test "type cast", cxt do
      assert {:ok, expr} = expression(cxt, "this.user_id::text = auth.user_id")

      assert Client.sql_expr(expr, :insert, cxt.vals) ==
               "NEW.user_id = '56f59cd3-566e-47bb-9c13-9dffdf6faa85'"

      assert Client.sql_expr(expr, :delete, cxt.vals) ==
               "OLD.user_id = '56f59cd3-566e-47bb-9c13-9dffdf6faa85'"

      assert Client.sql_expr(expr, :update, cxt.vals) ==
               "(NEW.user_id = '56f59cd3-566e-47bb-9c13-9dffdf6faa85') AND (OLD.user_id = '56f59cd3-566e-47bb-9c13-9dffdf6faa85')"
    end

    test "string concat", cxt do
      assert {:ok, expr} = expression(cxt, "this.user_id::text || 'cow' = auth.user_id || 'cow'")

      assert Client.sql_expr(expr, :insert, cxt.vals) ==
               "(NEW.user_id || 'cow') = ('56f59cd3-566e-47bb-9c13-9dffdf6faa85' || 'cow')"
    end

    test "known functions", cxt do
      assert_client_expr(cxt, "-row.amount = -1", "-(NEW.amount) = -1")
      assert_client_expr(cxt, "row.percent / 100.0 > 0.3", "(NEW.percent / 100.0) > 0.3")
    end
  end
end
