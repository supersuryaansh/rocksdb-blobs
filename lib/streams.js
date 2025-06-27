const { Writable } = require('streamx')

class BlobWriteStream extends Writable {
  constructor (db) {
    super()
    this.db = db
    this.tx = null
    this.digestPromise = null
    this.state = null
    this.id = null
  }

  _open (cb) {
    this.tx = this.db.transaction()

    this.digestPromise = this.tx.get('@blobs/digest').then(d => d || {
      blocks: 0,
      bytes: 0,
      blockSize: 65536
    })

    this.state = {
      blockSize: 65536,
      digest: null,
      id: null
    }

    cb()
  }

  async _write (chunk, cb) {
    if (!this.state.digest) {
      this.state.digest = await this.digestPromise
      this.state.blockSize = this.state.digest.blockSize
      this.state.id = {
        blockOffset: this.state.digest.blocks,
        blockLength: 0,
        byteOffset: this.state.digest.bytes,
        byteLength: 0
      }
    }

    let offset = 0
    while (offset < chunk.length) {
      const end = Math.min(offset + this.state.blockSize, chunk.length)
      const slice = chunk.subarray(offset, end)
      offset = end

      await this.tx.insert('@blobs/blocks', {
        index: this.state.digest.blocks++,
        value: slice
      })

      this.state.id.blockLength++
      this.state.id.byteLength += slice.length
      this.state.digest.bytes += slice.length
    }

    cb()
  }

  async _final (cb) {
    try {
      await this.tx.insert('@blobs/digest', this.state.digest)
      await this.tx.flush()
      this.id = this.state.id
      cb()
    } catch (err) {
      cb(err)
    }
  }
}

function WriteStream (db) {
  return new BlobWriteStream(db)
}

module.exports = { WriteStream }
