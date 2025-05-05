import { S3Client, GetObjectCommand, PutObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3'
import AsyncLock from 'async-lock'
import { proto } from '../../WAProto'
import { AuthenticationCreds, AuthenticationState, SignalDataTypeMap } from '../Types'
import { initAuthCreds } from './auth-utils'
import { BufferJSON } from './generics'

// We need to lock operations due to concurrent access
const operationLock = new AsyncLock({ maxPending: Infinity })

interface S3Config {
    bucket: string
    prefix?: string
    s3Config: any // AWS S3 client configuration
}

/**
 * Stores the full authentication state in an S3 bucket.
 * Similar to useMultiFileAuthState but uses S3 for storage.
 * 
 * @param config - S3 configuration
 * @param config.bucket - S3 bucket name
 * @param config.prefix - Optional prefix for all keys in the bucket
 * @param config.s3Config - AWS S3 client configuration
 */
export const useS3AuthState = async (config: S3Config): Promise<{ state: AuthenticationState, saveCreds: () => Promise<void> }> => {
    const { bucket, prefix = '', s3Config } = config
    const s3Client = new S3Client(s3Config)

    const writeData = async (data: any, key: string) => {
        const objectKey = `${prefix}${fixFileName(key)}`
        return operationLock.acquire(objectKey, async () => {
            const params = {
                Bucket: bucket,
                Key: objectKey,
                Body: JSON.stringify(data, BufferJSON.replacer),
                ContentType: 'application/json'
            }
            await s3Client.send(new PutObjectCommand(params))
        })
    }

    const readData = async (key: string) => {
        try {
            const objectKey = `${prefix}${fixFileName(key)}`
            return await operationLock.acquire(objectKey, async () => {
                const params = {
                    Bucket: bucket,
                    Key: objectKey
                }
                const response = await s3Client.send(new GetObjectCommand(params))
                const data = await response.Body?.transformToString()
                return data ? JSON.parse(data, BufferJSON.reviver) : null
            })
        } catch (error) {
            return null
        }
    }

    const removeData = async (key: string) => {
        try {
            const objectKey = `${prefix}${fixFileName(key)}`
            await operationLock.acquire(objectKey, async () => {
                const params = {
                    Bucket: bucket,
                    Key: objectKey
                }
                await s3Client.send(new DeleteObjectCommand(params))
            })
        } catch (_) {
            // Ignore errors on deletion
        }
    }

    const fixFileName = (file?: string) => file?.replace(/\//g, '__')?.replace(/:/g, '-') || ''

    const creds: AuthenticationCreds = await readData('creds.json') || initAuthCreds()

    return {
        state: {
            creds,
            keys: {
                get: async (type, ids) => {
                    const data: { [_: string]: SignalDataTypeMap[typeof type] } = {}
                    await Promise.all(ids.map(async (id) => {
                        let value = await readData(`${type}-${id}.json`)
                        if (type === 'app-state-sync-key' && value) {
                            value = proto.Message.AppStateSyncKeyData.fromObject(value)
                        }
                        data[id] = value
                    }))
                    return data
                },
                set: async(data) => {
                    const tasks: Promise<void>[] = []
                    for (const category in data) {
                        for (const id in data[category]) {
                            const value = data[category][id]
                            const key = `${category}-${id}.json`
                            tasks.push(value ? writeData(value, key) : removeData(key))
                        }
                    }
                    await Promise.all(tasks)
                }
            }
        },
        saveCreds: () => {
            return writeData(creds, 'creds.json')
        }
    }
} 