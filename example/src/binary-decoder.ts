/**
 * Pure JS Binary Decoder
 *
 * Decodes binary data from Worker using schema layout.
 * No WASM dependency - works entirely in main thread.
 */

import type { Stock } from './db'
import type { SerializedSchemaLayout } from './db.worker'

// Data type constants (must match Rust BinaryDataType)
const DataType = {
  Boolean: 0,
  Int32: 1,
  Int64: 2,
  Float64: 3,
  String: 4,
  DateTime: 5,
  Bytes: 6,
  Jsonb: 7,
} as const

// Header size in bytes
const HEADER_SIZE = 16

const textDecoder = new TextDecoder()

/**
 * Create a decoder function for the given schema layout.
 * Returns a function that decodes binary buffers to Stock arrays.
 */
export function createDecoder(layout: SerializedSchemaLayout): (buffer: ArrayBuffer) => Stock[] {
  const { columnCount, columnNames, columnTypes, columnOffsets, nullMaskSize } = layout

  return function decode(buffer: ArrayBuffer): Stock[] {
    const uint8Array = new Uint8Array(buffer)
    const dataView = new DataView(buffer)

    // Parse header
    const rowCount = dataView.getUint32(0, true)
    const rowStride = dataView.getUint32(4, true)
    const varOffset = dataView.getUint32(8, true)
    // const flags = dataView.getUint32(12, true)

    const stocks: Stock[] = new Array(rowCount)

    for (let rowIdx = 0; rowIdx < rowCount; rowIdx++) {
      const rowOffset = HEADER_SIZE + rowIdx * rowStride
      const dataOffset = rowOffset + nullMaskSize

      const row: Record<string, unknown> = {}

      for (let colIdx = 0; colIdx < columnCount; colIdx++) {
        const colName = columnNames[colIdx]
        const colType = columnTypes[colIdx]
        const colOffset = columnOffsets[colIdx]
        const offset = dataOffset + colOffset

        switch (colType) {
          case DataType.Boolean:
            row[colName] = dataView.getUint8(offset) !== 0
            break
          case DataType.Int32:
            row[colName] = dataView.getInt32(offset, true)
            break
          case DataType.Int64:
          case DataType.Float64:
            row[colName] = dataView.getFloat64(offset, true)
            break
          case DataType.DateTime:
            row[colName] = new Date(dataView.getFloat64(offset, true))
            break
          case DataType.String: {
            const strOffset = dataView.getUint32(offset, true)
            const strLength = dataView.getUint32(offset + 4, true)
            if (strLength === 0) {
              row[colName] = ''
            } else {
              row[colName] = textDecoder.decode(
                uint8Array.subarray(varOffset + strOffset, varOffset + strOffset + strLength)
              )
            }
            break
          }
          case DataType.Bytes: {
            const bytesOffset = dataView.getUint32(offset, true)
            const bytesLength = dataView.getUint32(offset + 4, true)
            if (bytesLength === 0) {
              row[colName] = new Uint8Array(0)
            } else {
              row[colName] = uint8Array.slice(
                varOffset + bytesOffset,
                varOffset + bytesOffset + bytesLength
              )
            }
            break
          }
          case DataType.Jsonb: {
            const jsonOffset = dataView.getUint32(offset, true)
            const jsonLength = dataView.getUint32(offset + 4, true)
            if (jsonLength === 0) {
              row[colName] = null
            } else {
              row[colName] = JSON.parse(
                textDecoder.decode(
                  uint8Array.subarray(varOffset + jsonOffset, varOffset + jsonOffset + jsonLength)
                )
              )
            }
            break
          }
          default:
            row[colName] = null
        }
      }

      stocks[rowIdx] = row as Stock
    }

    return stocks
  }
}
