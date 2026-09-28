import { useState } from 'react'

// Follow incoming values until edited; background refreshes must not erase a
// draft. Changing resource identity always starts a new draft.
export function useDraftField<T extends string | boolean>(source: T, owner: string = '') {
  const [state, setState] = useState({ owner, source, value: source })
  let current = state
  if (owner !== state.owner || source !== state.source) {
    current = {
      owner,
      source,
      value: owner !== state.owner || state.value === state.source ? source : state.value,
    }
    setState(current)
  }
  function setValue(value: T) {
    setState({ owner, source, value })
  }
  return [current.value, setValue] as const
}
