import url from 'url'
import {map} from 'rxjs/operator/map'
import {_do} from 'rxjs/operator/do'
import {retry} from 'rxjs/operator/retry'
import {publishReplay} from 'rxjs/operator/publishReplay'
import {httpGet, mkdirp, rename} from './util'
import assert from 'assert'
import {Observable} from 'rxjs/Observable'
import {spawn} from 'child_process'
import * as cache from './cache'
import tar from 'tar-fs'
import path from 'path'
import {mergeMap} from 'rxjs/operator/mergeMap'
import zlib from 'zlib'
import debuglog from './debuglog'
import {_catch} from 'rxjs/operator/catch'
import {cacheDir, sh, shFlag} from './config'
import {ArrayObservable} from 'rxjs/observable/ArrayObservable'
import {concat} from 'rxjs/operator/concat'
import {concatAll} from 'rxjs/operator/concatAll'

const log = debuglog('registry')

/**
 * default registry URL to be used. can be overridden via options on relevant
 * functions.
 * @type {String}
 */
export const DEFAULT_REGISTRY = 'https://registry.npmjs.org/'

/**
 * default number of retries to attempt before failing to resolve to a package
 * @type {Number}
 */
export const DEFAULT_RETRIES = 5

/**
 * register of pending and completed HTTP requests mapped to their respective
 * observable sequences.
 * @type {Object}
 */
export const requests = Object.create(null)

/**
 * clear the internal cache used for pending and completed HTTP requests.
 */
export function reset () {
	const uris = Object.keys(requests)
	for (const uri of uris) {
		delete requests[uri]
	}
}

/**
 * ensure that the registry responded with an accepted HTTP status code
 * (`200`).
 * @param  {String} uri - URI used for retrieving the supplied response.
 * @param  {Number} resp - HTTP response object.
 * @throws {assert.AssertionError} if the status code is not 200.
 */
export function checkStatus (uri, resp) {
	const {statusCode, body: {error}} = resp
	assert.equal(statusCode, 200, `error status code ${uri}: ${error}`)
}

/**
 * escape the given package name, which can then be used as part of the package
 * root URL.
 * @param  {String} name - package name.
 * @return {String} - escaped package name.
 */
export function escapeName (name) {
	const isScoped = name.charAt(0) === '@'
	const escapedName = isScoped
		? `@${encodeURIComponent(name.substr(1))}`
		: encodeURIComponent(name)
	return escapedName
}

/**
 * HTTP GET the resource at the supplied URI. if a request to the same URI has
 * already been made, return the cached (pending) request.
 * @param  {String} uri - endpoint to fetch data from.
 * @param  {Object} [options = {}] - optional HTTP and `retries` options.
 * @return {Observable} - observable sequence of pending / completed request.
 */
export function fetch (uri, options = {}) {
	const {retries = DEFAULT_RETRIES, ...needleOptions} = options
	const existingRequest = requests[uri]

	if (existingRequest) {
		return existingRequest
	}
	const newRequest = httpGet(uri, needleOptions)
		::_do((resp) => checkStatus(uri, resp))
		::retry(retries)::publishReplay().refCount()
	requests[uri] = newRequest
	return newRequest
}

/**
 * resolve a package defined via an ambiguous semantic version string to a
 * specific `package.json` file.
 * @param {String} name - package name.
 * @param {String} version - semantic version string or tag name.
 * @param {Object} options - HTTP request options.
 * @return {Observable} - observable sequence of the `package.json` file.
 */
export function match (name, version, options = {}) {
	const escapedName = escapeName(name)
	const {registry = DEFAULT_REGISTRY, ...fetchOptions} = options
	const uri = url.resolve(registry, `${escapedName}/${version}`)
	return fetch(uri, fetchOptions)::map(({body}) => body)
}

/**
 * get the commit ID associated with a specific revision on a git repository.
 * @param {String} repo - repository URL.
 * @param {String} ref - revision, tag, or branch identifier.
 * @return {Observable} - observable sequence of the commit ID as a string.
 */
export function getGitCommitId (repo, ref) {
	return Observable.create((observer) => {
		log(`fetching commit ID for ${repo}#${ref}`)
		let commitId = ''
		const childProcess = spawn('git', ['ls-remote', '--exit-code', repo, ref])
		childProcess.stdout.on('data', (data) => {
			commitId += data
		})
		childProcess.on('close', (code) => {
			if (code !== 0) {
				observer.error(new Error(
					`git ls-remote for ${repo}#${ref} exited with non-zero exit code: ${code}`
				))
				return
			}
			observer.next(commitId.substr(0, 40))
			observer.complete()
		})
		childProcess.on('error', (err) => {
			observer.error(err)
		})
	})
}

/**
 * clone a git repository at the specified revision to a temp directory. uses cache if available.
 * @param {String} repo - repository URL.
 * @param {String} commitId - commit ID.
 * @return {Observable} - observable sequence with the contents of the package.json file.
 */
export function clone (repo, commitId) {
	const tmpDir = cache.getTmp()
	const packageDir = path.join(tmpDir, 'package')
	log(`attempting to extract ${repo}#${commitId} from cache`)
	return cache.extract(packageDir, commitId)::_catch((error) => {	// eslint-disable-line
		if (error.code !== 'ENOENT') {
			throw error
		}
		log(`cloning ${repo}#${commitId} to temp directory ${tmpDir}`)
		return mkdirp(packageDir)::mergeMap(() => Observable.create((observer) => {
			// try to ensure we only fetch the minimum needed for that particular commit.
			// REVIEW: can this be made faster?
			const shellCmd = `
			cd ${packageDir} &&
			git init &&
			git remote add origin ${repo} &&
			git fetch origin &&
			git checkout ${commitId}
			`
			const childProcess = spawn(sh, [shFlag, shellCmd])
			childProcess.on('error', (err) => {
				observer.error(err)
			})
			childProcess.on('close', (code) => {
				if (code !== 0) {
					observer.error(new Error(
						`git clone for ${repo}#${commitId} exited with non-zero exit code: ${code}`
					))
					return
				}
				observer.next()
				observer.complete()
			})
		// make sure to save the tarball to cache after the clone.
		})
		::mergeMap(() => Observable.create((observer) => {
			log(`saving to cache: ${tmpDir}`)
			const packed = tar.pack(tmpDir, {
				ignore: (name) => name === '.git'	// do not add the .git dir to the tarball
			}).pipe(zlib.createGzip()).pipe(cache.write())
			packed.on('error', (err) => {
				observer.error(err)
			})
			packed.on('finish', () => {
				observer.next(packed.path)
				observer.complete()
			})
		}))
		::map((tmpPath) => {
			log(`${tmpPath} => ${path.join(cacheDir, commitId)}`)
			return rename(tmpPath, path.join(cacheDir, commitId))
		}))
	})
	::concat([ArrayObservable.create([packageDir])])
	::concatAll()
}
