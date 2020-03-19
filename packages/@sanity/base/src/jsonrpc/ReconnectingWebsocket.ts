const hasWebSocket = typeof window !== 'undefined' && typeof window.WebSocket === 'function'

type WebsocketData = string | ArrayBuffer | SharedArrayBuffer | Blob | ArrayBufferView
type EventHandler = {
  type: keyof WebSocketEventMap
  listener: (this: WebSocket, ev: any) => any
  options: boolean | AddEventListenerOptions
}

class ReconnectingWebsocket {
  private ws: WebSocket
  private url: string
  private isOpen: boolean
  private isClosed: boolean
  private lastHeartbeat: number
  private sendQueue: WebsocketData[]
  private eventHandlers: EventHandler[]

  constructor(url: string) {
    this.url = url
    this.isClosed = false
    this.sendQueue = []
    this.eventHandlers = []
    this.reconnect()
  }

  onOpen = (): void => {
    this.isOpen = true

    let packet: WebsocketData
    while ((packet = this.sendQueue.shift())) {
      this.ws.send(packet)
    }
  }

  onClose = (): void => {
    this.isOpen = false
    console.warn('Disconnected, but manually closed')
    if (this.isClosed) {
      return
    }

    this.reconnect()
  }

  onHeartbeat = (): void => {
    this.lastHeartbeat = Date.now()
  }

  reconnect(): void {
    // Clean up any previous websocket
    this.teardown()

    // Create new one
    this.ws = new WebSocket(this.url)
    this.ws.addEventListener('open', this.onOpen)
    this.ws.addEventListener('close', this.onClose)
    this.ws.addEventListener('message', this.onHeartbeat)
    this.eventHandlers.forEach(handler => {
      this.ws.addEventListener(handler.type, handler.listener, handler.options)
    })
  }

  teardown(): void {
    if (!this.ws) {
      return
    }

    this.ws.removeEventListener('open', this.onOpen)
    this.ws.removeEventListener('close', this.onClose)
    this.eventHandlers.forEach(handler => {
      this.ws.removeEventListener(handler.type, handler.listener, handler.options)
    })
    this.ws.close()
  }

  close(code?: number, reason?: string): void {
    this.isClosed = true
    return this.ws.close(code, reason)
  }

  send(data: WebsocketData): void {
    if (!this.isOpen) {
      this.sendQueue.push(data)
      return
    }

    return this.ws.send(data)
  }

  addEventListener<K extends keyof WebSocketEventMap>(
    type: K,
    listener: (this: WebSocket, ev: WebSocketEventMap[K]) => any,
    options?: boolean | AddEventListenerOptions
  ): void {
    this.eventHandlers.push({type, listener, options})
    return this.ws.addEventListener(type, listener, options)
  }

  removeEventListener<K extends keyof WebSocketEventMap>(
    type: K,
    listener: (this: WebSocket, ev: WebSocketEventMap[K]) => any,
    options?: boolean | EventListenerOptions
  ): void {
    const handlerIndex = this.eventHandlers.findIndex(
      candidate => candidate.type === type && candidate.listener === listener
    )

    if (handlerIndex !== -1) {
      this.eventHandlers.splice(handlerIndex, 1)
    }

    return this.ws.removeEventListener(type, listener, options)
  }
}

export default hasWebSocket ? ReconnectingWebsocket : undefined
