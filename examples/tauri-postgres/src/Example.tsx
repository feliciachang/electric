import React, { useEffect, useState } from 'react'

import { makeElectricContext, useLiveQuery } from 'electric-sql/react'
// import { ElectricDatabase, electrify } from 'electric-sql/wa-sqlite'
import { ElectricDatabase, electrify } from 'electric-sql/sqlx'

import { invoke } from '@tauri-apps/api'

import { authToken } from 'electric-sql/auth'
import { genUUID } from 'electric-sql/util'

import { Electric, Item, schema } from './generated/client'

import './Example.css'

const { ElectricProvider, useElectric } = makeElectricContext<Electric>()

const localAuthToken = (): Promise<string> => {
  const issuer = 'local-development'
  const signingKey = 'local-development-key-minimum-32-symbols'

  return authToken(issuer, signingKey)
}

export const Example = () => {
  const [ electric, setElectric ] = useState<Electric>()

  useEffect(() => {
    let isMounted = true

    const init = async () => {
      const config = {
        auth: {
          token: await localAuthToken()
        }
      }

      const conn = await ElectricDatabase.init('electric.db', '', invoke)
      const electric = await electrify(conn, schema, config)

      if (!isMounted) {
        return
      }

      setElectric(electric)
    }

    init()

    return () => {
      isMounted = false
    }
  }, [])

  if (electric === undefined) {
    return null
  }

  return (
    <ElectricProvider db={electric}>
      <ExampleComponent />
    </ElectricProvider>
  )
}

const ExampleComponent = () => {
  const { db } = useElectric()!

  useEffect(() => void db.items.sync(), [])

  const { results } = useLiveQuery(
    db.items.liveMany()
  )

  const addItem = async () => {
    await db.items.create({
      data: {
        value: genUUID(),
        // uncomment the line below after migration
        //other_value: genUUID(),
      }
    })
  }

  const clearItems = async () => {
    await db.items.deleteMany() // delete all items
  }

  const items: Item[] = results ?? []

  // After the migration, comment out this code and uncomment code block below
  return (
    <div>
      <div className="controls">
        <button className="button" onClick={addItem}>
          Add
        </button>
        <button className="button" onClick={clearItems}>
          Clear
        </button>
      </div>
      {items.map((item: any, index: any) => (
        <p key={ index } className="item">
          <code>{ item.value }</code>
        </p>
      ))}
    </div>
  )

  // Uncomment after migration
  //return (
  //  <div>
  //    <div className="controls">
  //      <button className="button" onClick={addItem}>
  //        Add
  //      </button>
  //      <button className="button" onClick={clearItems}>
  //        Clear
  //      </button>
  //    </div>
  //    {items.map((item: any, index: any) => (
  //      <p key={ index } className="item">
  //        <code>{ item.value } - { item.other_value }</code>
  //      </p>
  //    ))}
  //  </div>
  //)
}