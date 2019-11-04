/* eslint-disable import/prefer-default-export */

// old: authors;knut,{"template":"diaryEntry"}
// new: authors;knut,view=diff,eyJyZXYxIjoiYWJjMTIzIiwicmV2MiI6ImRlZjQ1NiJ9|latest-posts

const panePattern = /^([a-z0-9_-]+),?({.*?})?(?:(;|$))/i
const isParam = str => /^[a-z0-9]+=[^=]+/i.test(str)
const isPayload = str =>
  /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(str)

export function parsePanesSegment(str) {
  if (str.indexOf(',{') !== -1) {
    return parseOldPanesSegment(str)
  }

  return str
    .split(';')
    .map(group =>
      group
        .split('|')
        .filter(Boolean)
        .map(segment => {
          const [id, ...chunks] = segment.split(',')
          return chunks.reduce(
            (pane, chunk) => {
              if (isParam(chunk)) {
                const key = chunk.slice(0, chunk.indexOf('='))
                const value = chunk.slice(key.length + 1)
                pane.params[key] = value
              } else if (isPayload(chunk)) {
                pane.payload = tryParseBase64Payload(chunk)
              } else {
                // eslint-disable-next-line no-console
                console.warn('Unknown pane segment: %s - skipping', segment)
              }

              return pane
            },
            {id, params: {}, payload: undefined}
          )
        })
    )
    .filter(group => group.length > 0)
}

export function encodePanesSegment(panes = []) {
  return panes
    .map(group => {
      return group
        .map(({id, params = {}, payload}) => {
          const encodedPayload =
            typeof payload === 'undefined' ? undefined : btoa(JSON.stringify(payload))

          const encodedParams = Object.keys(params).reduce(
            (pairs, key) => [...pairs, `${key}=${params[key]}`],
            []
          )

          return [id, encodedParams.length > 0 && encodedParams, encodedPayload]
            .filter(Boolean)
            .join(',')
        })
        .join('|')
    })
    .map(encodeURIComponent)
    .join(';')
}

export function parseOldPanesSegment(str) {
  const chunks = []

  let buffer = str
  while (buffer.length) {
    const [match, id, payloadChunk] = buffer.match(panePattern) || []
    if (!match) {
      buffer = buffer.slice(1)
      continue
    }

    const payload = payloadChunk && tryParsePayload(payloadChunk)
    chunks.push({id, payload})

    buffer = buffer.slice(match.length)
  }

  return chunks
}

function tryParsePayload(json) {
  try {
    return JSON.parse(json)
  } catch (err) {
    // eslint-disable-next-line no-console
    console.warn(`Failed to parse parameters: ${err.message}`)
    return undefined
  }
}

function tryParseBase64Payload(data) {
  return tryParsePayload(atob(data))
}