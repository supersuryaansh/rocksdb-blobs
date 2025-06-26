const RW = require('read-write-mutexify')
const spec = require('./spec/db')
const HyperDB = require('hyperdb')
const b4a = require('b4a')
const { Readable, Writable } = require('streamx')

module.exports = class RocksBlobs {
  constructor (storage) {
    this.db = HyperDB.rocks(storage, spec)
    this.rw = new RW()
  }

  ready () {
    return this.db.ready()
  }

  close () {
    return this.db.close()
  }

  async put (buffer) {
    await this.rw.write.lock()
    const tx = this.db.transaction()

    try {
      const digest = (await tx.get('@blobs/digest')) || { blocks: 0, bytes: 0, blockSize: 65536 }
      const id = { blockOffset: digest.blocks, blockLength: 0, byteOffset: digest.bytes, byteLength: 0 }

      let offset = 0
      while (offset < buffer.byteLength) {
        const value = buffer.subarray(offset, offset += digest.blockSize)

        offset += value.byteLength
        digest.bytes += value.byteLength

        id.blockLength++
        id.byteLength += value.byteLength

        await tx.insert('@blobs/blocks', { index: digest.blocks++, value })
      }

      await tx.insert('@blobs/digest', digest)
      await tx.flush()

      return id
    } catch (err) {
      await tx.close()
      throw err
    } finally {
      await this.rw.write.unlock()
    }
  }

  async get (id) {
    if (id.blockLength === 1) {
      const blk = await this.db.get('@blobs/blocks', { index: id.blockOffset })
      return blk && blk.value
    }

    const all = []

    this.db.cork()
    for (let i = 0; i < id.blockLength; i++) {
      all.push(this.db.get('@blobs/blocks', { index: id.blockOffset + i }))
    }
    this.db.uncork()

    const blocks = await Promise.all(all)
    const bufs = new Array(blocks.length)

    for (let i = 0; i < blocks.length; i++) {
      if (blocks[i] === null) return null
      bufs[i] = blocks[i].value
    }

    return b4a.concat(bufs)
  }

  createWriteStream () {
    const tx = this.db.transaction()
    const digestPromise = tx.get('@blobs/digest').then(d => d || {
      blocks: 0,
      bytes: 0,
      blockSize: 65536
    })

    const state = {
      blockSize: 65536,
      digest: null,
      id: null
    }

    const stream = new Writable({
      async write (chunk) {
        console.log('writable of stream called')
        if (!state.digest) {
          state.digest = await digestPromise
          state.blockSize = state.digest.blockSize
          state.id = {
            blockOffset: state.digest.blocks,
            blockLength: 0,
            byteOffset: state.digest.bytes,
            byteLength: 0
          }
        }

        let offset = 0
        while (offset < chunk.length) {
          const end = Math.min(offset + state.blockSize, chunk.length)
          const slice = chunk.subarray(offset, end)
          offset = end

          await tx.insert('@blobs/blocks', {
            index: state.digest.blocks++,
            value: slice
          })

          state.id.blockLength++
          state.id.byteLength += slice.length
          state.digest.bytes += slice.length
        }
      },

      // Explicitly override _final with callback signature
      final (cb) {
        console.log('final execs')
        (async () => {
          try {
            console.log('[final()] called')
            await tx.insert('@blobs/digest', state.digest)
            await tx.flush()
            stream.id = state.id
            console.log('[final()] stream.id =', stream.id)
            cb()
          } catch (err) {
            console.log('final errors')
            cb(err)
          }
        })()
      },

      autoDestroy: true
    })

    return stream
  }

  createReadStream (id) {
    let currentBlock = 0

    const stream = new Readable({
      async read () {
        if (currentBlock >= id.blockLength) {
          this.push(null) // End of stream
          return
        }

        try {
          const blockIndex = id.blockOffset + currentBlock
          const entry = await this.db.get('@blobs/blocks', { index: blockIndex })

          if (!entry || !entry.value) {
            this.destroy(new Error(`Missing block at index ${blockIndex}`))
            return
          }

          this.push(entry.value)
          currentBlock++
        } catch (err) {
          this.destroy(err)
        }
      }
    })

    return stream
  }
}
